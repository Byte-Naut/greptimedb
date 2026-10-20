// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable byte pages backed by native cache loading and bounded range reads.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::io;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use common_base::range_read::{BatchConfig, BatchEvent, BatchReader, RangeReader, ReadPlan};
use futures::future;
use prometheus::IntGauge;

use crate::accounting::CacheAccounting;
use crate::metrics::CacheMetrics;
use crate::page::PageLayout;
use crate::{Cache, RemovalCause};

/// Business-level events; callers retain their existing hit/miss metric units.
pub enum PageEvent {
    /// Unique logical page lookups in one request, before any missing loads execute.
    Lookup { hits: usize, misses: usize },
    /// One successful native page loader, excluding waiters and hits.
    Loaded { bytes: u64 },
    /// A page larger than the bounded queue's supported size is read directly.
    UnbatchedRead,
    /// One bounded retry after a previous owner dropped its read driver.
    ClosedRetry,
    /// Backend weight, not process RSS; sampled after fetch/maintenance.
    Residency { usage: u64, capacity: u64 },
    /// Backend eviction, excluding unadmitted values released by their owners.
    Evicted { cause: RemovalCause },
}

/// Caller-bound metrics without storage-engine dependencies or global registrations.
#[derive(Clone)]
pub struct PageCacheObserver {
    pub bytes: IntGauge,
    pub loads: Option<Arc<CacheMetrics>>,
    pub events: Arc<dyn Fn(PageEvent) + Send + Sync>,
    pub batches: Arc<dyn Fn(BatchEvent) + Send + Sync>,
}

/// Byte accounting for one successful fetch. Hits are initial lookups, not waiters.
#[derive(Debug, Default)]
pub struct PageFetchStats {
    pub cache_hit: usize,
    pub cache_miss: usize,
    /// Requested intersections already cached; overlapping requests count repeatedly.
    pub cached_bytes: u64,
    pub cached_fragments: usize,
    /// Original request lengths, including duplicates and overlaps.
    pub requested_bytes: u64,
    /// Sum of unique logical page lengths, clipped at EOF, including cache hits.
    pub aligned_bytes: u64,
    /// Union of original request byte ranges, excluding overlaps.
    pub unique_requested_bytes: u64,
}

/// Caches fixed-size byte pages of immutable content identified by `K`.
/// File identity must include any version needed to distinguish different contents.
/// The queue bounds in-flight requests, not the total response or cache memory.
/// Oversized legacy pages may bypass the queue and require a full-page allocation.
pub struct PagedCache<K> {
    cache: Cache<(K, u64), Arc<CachedPage>>,
    page_size: u64,
    weigher: PageWeigher<K>,
    observer: PageCacheObserver,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> PagedCache<K> {
    /// Creates an independent cache budget. Zero page size is rejected by fetch.
    pub fn new(
        name: &str,
        capacity: u64,
        page_size: u64,
        weigher: impl Fn(&(K, u64), &Bytes) -> u32 + Send + Sync + 'static,
        observer: PageCacheObserver,
    ) -> Self {
        let weigher: PageWeigher<K> = Arc::new(weigher);
        let weight = weigher.clone();
        let events = observer.events.clone();
        let mut cache = Cache::new(
            name,
            capacity,
            move |key, value: &Arc<CachedPage>| weight(key, &value.bytes),
            move |_, value, cause| {
                value.accounting.release();
                events(PageEvent::Evicted { cause });
            },
        );
        if let Some(metrics) = &observer.loads {
            cache = cache.with_metrics(metrics.clone());
        }
        Self {
            cache,
            page_size,
            weigher,
            observer,
        }
    }

    /// Invalidates completed pages. Concurrent loads are not fenced.
    pub fn invalidate_if(
        &self,
        predicate: impl Fn(&K) -> bool + Send + Sync + 'static,
    ) -> io::Result<()> {
        self.cache
            .invalidate_if(move |(file, _), _| predicate(file))
    }

    /// Looks up a completed page without updating business lookup counters.
    pub async fn contains(&self, file: &K, page_id: u64) -> bool {
        self.cache.get(&(file.clone(), page_id)).await.is_some()
    }

    /// Recovers immutable file size from a requested page already in memory.
    pub async fn known_file_size(&self, file: &K, ranges: &[Range<u64>]) -> Option<u64> {
        let layout = PageLayout::new(NonZeroU64::new(self.page_size)?, u64::MAX);
        for range in ranges {
            for id in layout.page_ids(range.clone()).ok()? {
                if let Some(page) = self.cache.get(&(file.clone(), id)).await {
                    return Some(page.file_size);
                }
            }
        }
        None
    }

    /// Applies pending cache admission and eviction work.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
        self.observe_residency();
    }

    /// Fetches bytes in input order. The reader must address the same immutable
    /// content as `file`; this call's batch driver never merges different files.
    pub async fn fetch<R: RangeReader + ?Sized>(
        &self,
        file: K,
        file_size: u64,
        ranges: &[Range<u64>],
        reader: &R,
    ) -> Result<(Vec<Bytes>, PageFetchStats), Arc<io::Error>> {
        if ranges.iter().any(|r| r.start > r.end || r.end > file_size) {
            return Err(invalid_request());
        }
        let page_size = NonZeroU64::new(self.page_size).ok_or_else(invalid_request)?;
        let layout = PageLayout::new(page_size, file_size);
        let mut stats = PageFetchStats::default();
        let mut ids = BTreeSet::new();
        for range in ranges {
            stats.requested_bytes = stats
                .requested_bytes
                .checked_add(range.end - range.start)
                .ok_or_else(invalid_request)?;
            ids.extend(layout.page_ids(range.clone()).map_err(Arc::new)?);
        }
        let plan = ReadPlan::new(ranges, NonZeroU64::MAX, 0).map_err(Arc::new)?;
        stats.unique_requested_bytes = plan.ranges().iter().map(|r| r.end - r.start).sum();
        let mut pages = BTreeMap::new();
        let mut missing = Vec::new();
        for id in ids {
            let range = layout.page_range(id).ok_or_else(invalid_request)?;
            stats.aligned_bytes += range.end - range.start;
            if let Some(page) = self.cache.get(&(file.clone(), id)).await {
                check_file_size(&page, file_size)?;
                pages.insert(id, page.bytes.clone());
            } else {
                missing.push(id);
            }
        }
        for range in ranges {
            for id in layout.page_ids(range.clone()).map_err(Arc::new)? {
                if pages.contains_key(&id) {
                    let slice = layout
                        .request_slice(id, range.clone())
                        .map_err(Arc::new)?
                        .ok_or_else(invalid_request)?;
                    stats.cached_bytes += slice.len() as u64;
                    stats.cached_fragments += 1;
                }
            }
        }
        stats.cache_hit = pages.len();
        stats.cache_miss = missing.len();
        (self.observer.events)(PageEvent::Lookup {
            hits: stats.cache_hit,
            misses: stats.cache_miss,
        });
        if !missing.is_empty() {
            if let Ok(budget) = u32::try_from(self.page_size.min(file_size).max(16 * 1024 * 1024)) {
                let events = self.observer.batches.clone();
                let (queue, driver) = BatchReader::with_observer(
                    BatchConfig {
                        max_requests: 256,
                        max_requested_bytes: budget,
                        max_read_size: NonZeroU64::new(8 * 1024 * 1024)
                            .ok_or_else(invalid_request)?,
                        max_gap: 0,
                        max_concurrent_reads: 8,
                    },
                    move |event| events(event),
                )
                .map_err(Arc::new)?;
                let loads = future::try_join_all(missing.into_iter().map(|id| {
                    let queue = queue.clone();
                    let key = (file.clone(), id);
                    async move {
                        let range = layout.page_range(id).ok_or_else(invalid_request)?;
                        let mut retry = false;
                        let page = loop {
                            let result = self
                                .cache
                                .try_get_with(&key, async {
                                    let bytes = queue.read(range.clone()).await?;
                                    Ok::<_, io::Error>(
                                        self.create_cached_page(&key, file_size, bytes),
                                    )
                                })
                                .await;
                            if !retry
                                && result.as_ref().err().is_some_and(|error| {
                                    common_base::range_read::ReadQueue::is_closed_error(error)
                                })
                            {
                                retry = true;
                                (self.observer.events)(PageEvent::ClosedRetry);
                                continue;
                            }
                            break result?;
                        };
                        check_file_size(&page, file_size)?;
                        Ok::<_, Arc<io::Error>>((id, page.bytes.clone()))
                    }
                }));
                pages.extend(driver.drive(loads, reader).await?);
            } else {
                for id in missing {
                    let key = (file.clone(), id);
                    let range = layout.page_range(id).ok_or_else(invalid_request)?;
                    let page = self
                        .cache
                        .try_get_with(&key, async {
                            (self.observer.events)(PageEvent::UnbatchedRead);
                            let bytes = reader.read(range.clone()).await?;
                            if bytes.len() as u64 != range.end - range.start {
                                return Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "Invalid page response length",
                                ));
                            }
                            Ok(self.create_cached_page(
                                &key,
                                file_size,
                                Bytes::copy_from_slice(&bytes),
                            ))
                        })
                        .await?;
                    check_file_size(&page, file_size)?;
                    pages.insert(id, page.bytes.clone());
                }
            }
        }
        let mut output = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.is_empty() {
                output.push(Bytes::new());
                continue;
            }
            let ids = layout.page_ids(range.clone()).map_err(Arc::new)?;
            if ids.end - ids.start == 1 {
                let slice = layout
                    .request_slice(ids.start, range.clone())
                    .map_err(Arc::new)?
                    .ok_or_else(invalid_request)?;
                let page = pages.get(&ids.start).ok_or_else(invalid_request)?;
                if slice.end > page.len() {
                    return Err(invalid_request());
                }
                output.push(page.slice(slice));
                continue;
            }
            let len = usize::try_from(range.end - range.start).map_err(|_| invalid_request())?;
            let mut assembled = BytesMut::with_capacity(len);
            for id in ids {
                let slice = layout
                    .request_slice(id, range.clone())
                    .map_err(Arc::new)?
                    .ok_or_else(invalid_request)?;
                let page = pages.get(&id).ok_or_else(invalid_request)?;
                if slice.end > page.len() {
                    return Err(invalid_request());
                }
                assembled.extend_from_slice(&page[slice]);
            }
            if assembled.len() != len {
                return Err(invalid_request());
            }
            output.push(assembled.freeze());
        }
        self.observe_residency();
        Ok((output, stats))
    }

    fn create_cached_page(&self, key: &(K, u64), file_size: u64, bytes: Bytes) -> Arc<CachedPage> {
        (self.observer.events)(PageEvent::Loaded {
            bytes: bytes.len() as u64,
        });
        let accounting =
            CacheAccounting::new((self.weigher)(key, &bytes), self.observer.bytes.clone());
        Arc::new(CachedPage {
            file_size,
            bytes,
            accounting,
        })
    }

    fn observe_residency(&self) {
        (self.observer.events)(PageEvent::Residency {
            usage: self.cache.weighted_size(),
            capacity: self.cache.capacity(),
        });
    }
}

struct CachedPage {
    file_size: u64,
    bytes: Bytes,
    accounting: CacheAccounting,
}

type PageWeigher<K> = Arc<dyn Fn(&(K, u64), &Bytes) -> u32 + Send + Sync>;

fn invalid_request() -> Arc<io::Error> {
    Arc::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Invalid cached page range",
    ))
}

fn check_file_size(page: &CachedPage, expected: u64) -> Result<(), Arc<io::Error>> {
    if page.file_size != expected {
        return Err(Arc::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cached content identity used with inconsistent file sizes",
        )));
    }
    Ok(())
}
