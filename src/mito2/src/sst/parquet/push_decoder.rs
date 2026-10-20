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

//! Push decoder stream implementation for SST parquet files.

use std::io;
use std::ops::Range;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use common_base::range_read::{Metadata, RangeReader};
use tokio::sync::OnceCell;

use bytes::Bytes;
use common_recordbatch::QueryMemoryTracker;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use snafu::{OptionExt, ResultExt, ensure};

use crate::cache::CacheStrategy;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::file::RegionFileId;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// Fetches parquet byte ranges through Greptime's cache hierarchy.
///
/// The push decoder decides which ranges are required for decoding, while this
/// fetcher keeps cache lookup, local write-cache reads, and remote I/O explicit
/// in Greptime code.
pub struct SstParquetRangeFetcher {
    /// Region file ID for cache key.
    region_file_id: RegionFileId,
    /// Path to the parquet file in object storage.
    file_path: String,
    /// Object store for reading data.
    object_store: ObjectStore,
    /// Cache strategy for reading pages.
    cache_strategy: CacheStrategy,
    /// Immutable SST size, resolved once when no manifest hint is available.
    file_size: OnceCell<u64>,
    file_size_hint: u64,
    /// Optional metrics for tracking fetch operations.
    fetch_metrics: Option<ParquetFetchMetrics>,
    /// Shared quota for decoding this fetcher's row group.
    memory_tracker: Option<QueryMemoryTracker>,
}

impl SstParquetRangeFetcher {
    /// Creates a new [SstParquetRangeFetcher].
    pub fn new(
        region_file_id: RegionFileId,
        file_path: String,
        object_store: ObjectStore,
        cache_strategy: CacheStrategy,
        fetch_metrics: Option<ParquetFetchMetrics>,
    ) -> Self {
        Self {
            region_file_id,
            file_path,
            object_store,
            cache_strategy,
            file_size: OnceCell::new(),
            file_size_hint: 0,
            fetch_metrics,
            memory_tracker: None,
        }
    }

    /// Sets the shared quota for this decoder's projected row group.
    pub(crate) fn with_memory_tracker(mut self, tracker: Option<QueryMemoryTracker>) -> Self {
        self.memory_tracker = tracker;
        self
    }

    /// Supplies the immutable SST size from its manifest entry.
    pub(crate) fn with_file_size(mut self, size: u64) -> Self {
        self.file_size_hint = size;
        self
    }

    async fn resolve_file_size(&self, ranges: &[Range<u64>]) -> Result<u64> {
        self.file_size
            .get_or_try_init(|| async {
                if self.file_size_hint != 0 {
                    return Ok(self.file_size_hint);
                }
                if let Some(cache) = self.cache_strategy.parquet_page_cache()
                    && let Some(size) = cache.known_file_size(self.region_file_id, ranges).await
                {
                    return Ok(size);
                }
                if let Some(cache) = self.cache_strategy.write_cache() {
                    let key = IndexKey::new(
                        self.region_file_id.region_id(),
                        self.region_file_id.file_id(),
                        FileType::Parquet,
                    );
                    if let Some(entry) = cache.file_cache().get(key).await {
                        return Ok(u64::from(entry.file_size));
                    }
                }
                Ok(self
                    .object_store
                    .stat(&self.file_path)
                    .await
                    .context(OpenDalSnafu)?
                    .content_length())
            })
            .await
            .copied()
    }

    /// Fetches decoder ranges through fixed-size pages and the existing local/remote hierarchy.
    async fn fetch_bytes_with_cache(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let start = self.fetch_metrics.as_ref().map(|_| Instant::now());
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();
        ensure!(
            ranges.iter().all(|r| r.start <= r.end),
            UnexpectedSnafu {
                reason: "Reversed Parquet byte range"
            }
        );
        if ranges.iter().all(Range::is_empty) {
            return Ok(vec![Bytes::new(); ranges.len()]);
        }
        let reader = ParquetRangeReader {
            fetcher: self,
            local: self.fetch_metrics.as_ref().map(|_| Mutex::default()),
            remote: self.fetch_metrics.as_ref().map(|_| Mutex::default()),
        };
        let output = if let Some(cache) = self.cache_strategy.parquet_page_cache() {
            let size = self.resolve_file_size(ranges).await?;
            let (output, stats) = cache
                .fetch(self.region_file_id, size, ranges, &reader)
                .await
                .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)))
                .context(ReadParquetSnafu {
                    path: &self.file_path,
                })?;
            if let Some(metrics) = &self.fetch_metrics {
                let mut data = metrics.data.lock().unwrap();
                data.page_cache_hit += usize::from(stats.cached_bytes > 0);
                data.pages_to_fetch_mem += stats.cached_fragments;
                data.page_size_to_fetch_mem += stats.cached_bytes;
            }
            output
        } else {
            reader.read_ranges(ranges, false).await?
        };
        if let (Some(metrics), Some(local), Some(remote), Some(start)) =
            (&self.fetch_metrics, &reader.local, &reader.remote, start)
        {
            let mut data = metrics.data.lock().unwrap();
            let local = local.lock().unwrap();
            let remote = remote.lock().unwrap();
            data.write_cache_hit += usize::from(local.ranges != 0);
            data.cache_miss += usize::from(remote.ranges != 0);
            data.pages_to_fetch_write_cache += local.ranges;
            data.page_size_to_fetch_write_cache += local.bytes;
            data.write_cache_fetch_elapsed += local.elapsed();
            data.pages_to_fetch_store += remote.ranges;
            data.page_size_to_fetch_store += remote.bytes;
            data.store_fetch_elapsed += remote.elapsed();
            data.page_size_needed += output.iter().map(|b| b.len() as u64).sum::<u64>();
            data.total_fetch_elapsed += start.elapsed();
        }
        Ok(output)
    }
}

/// Aggregates one fetch's physical reads without turning request hit counters
/// into per-I/O counters. Elapsed time spans concurrent reads rather than summing them.
#[derive(Default)]
struct StorageReadMetrics {
    ranges: usize,
    bytes: u64,
    interval: Option<(Instant, Instant)>,
}

impl StorageReadMetrics {
    fn record(&mut self, ranges: usize, bytes: u64, start: Instant) {
        self.ranges += ranges;
        self.bytes += bytes;
        let end = Instant::now();
        self.interval = Some(match self.interval {
            Some((first, last)) => (first.min(start), last.max(end)),
            None => (start, end),
        });
    }

    fn elapsed(&self) -> std::time::Duration {
        self.interval
            .map(|(start, end)| end - start)
            .unwrap_or_default()
    }
}

/// One immutable SST view. The common batch reader controls its concurrency and
/// boundaries; it must not re-expand a planned range through gap merging.
struct ParquetRangeReader<'a> {
    fetcher: &'a SstParquetRangeFetcher,
    local: Option<Mutex<StorageReadMetrics>>,
    remote: Option<Mutex<StorageReadMetrics>>,
}

impl ParquetRangeReader<'_> {
    async fn read_ranges(&self, ranges: &[Range<u64>], planned: bool) -> Result<Vec<Bytes>> {
        let fetcher = self.fetcher;
        let key = IndexKey::new(
            fetcher.region_file_id.region_id(),
            fetcher.region_file_id.file_id(),
            FileType::Parquet,
        );
        let bytes = if fetcher.fetch_metrics.is_none() {
            0
        } else if planned {
            ranges.iter().map(|r| r.end - r.start).sum()
        } else {
            compute_total_range_size(ranges).1
        };
        let start = self.local.as_ref().map(|_| Instant::now());
        if let Some(cache) = fetcher.cache_strategy.write_cache()
            && let Some(data) = cache.file_cache().read_ranges(key, ranges).await
        {
            if let (Some(metrics), Some(start)) = (&self.local, start) {
                metrics.lock().unwrap().record(ranges.len(), bytes, start);
            }
            return Ok(data);
        }
        let _timer = READ_STAGE_ELAPSED
            .with_label_values(&["cache_miss_read"])
            .start_timer();
        let start = self.remote.as_ref().map(|_| Instant::now());
        let data = if planned {
            // RangeReader::read submits exactly one bounded range.
            let range = ranges.first().ok_or_else(|| {
                UnexpectedSnafu {
                    reason: "Empty planned Parquet read",
                }
                .build()
            })?;
            vec![
                fetcher
                    .object_store
                    .read_with(&fetcher.file_path)
                    .range(range.clone())
                    .await
                    .context(OpenDalSnafu)?
                    .to_bytes(),
            ]
        } else {
            fetch_byte_ranges(&fetcher.file_path, fetcher.object_store.clone(), ranges)
                .await
                .context(OpenDalSnafu)?
        };
        if let (Some(metrics), Some(start)) = (&self.remote, start) {
            metrics.lock().unwrap().record(ranges.len(), bytes, start);
        }
        Ok(data)
    }
}

#[async_trait]
impl RangeReader for ParquetRangeReader<'_> {
    async fn metadata(&self) -> io::Result<Metadata> {
        self.fetcher
            .resolve_file_size(&[])
            .await
            .map(|content_length| Metadata { content_length })
            .map_err(io::Error::other)
    }

    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut data = self
            .read_ranges(std::slice::from_ref(&range), true)
            .await
            .map_err(io::Error::other)?;
        if data.len() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Parquet read response count",
            ));
        }
        data.pop().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Missing Parquet read response",
            )
        })
    }
}

/// Builds a parquet record batch stream driven directly by [ParquetPushDecoderBuilder].
pub fn build_sst_parquet_record_batch_stream(
    arrow_metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    projection: ProjectionMask,
    fetcher: SstParquetRangeFetcher,
    file_path: String,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let uncompressed_bytes = if fetcher.memory_tracker.is_some() {
        let row_group = arrow_metadata.metadata().row_group(row_group_idx);
        row_group
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| projection.leaf_included(*index))
            .try_fold(0usize, |total, (_, column)| {
                usize::try_from(column.uncompressed_size())
                    .ok()
                    .and_then(|size| total.checked_add(size))
            })
            .context(UnexpectedSnafu {
                reason: "Invalid projected row-group uncompressed size",
            })?
    } else {
        0
    };
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection)
        .with_batch_size(batch_size);

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut decoder = builder
        .build()
        .context(ReadParquetSnafu { path: &file_path })?;

    Ok(async_stream::try_stream! {
        let _reservation = if let Some(tracker) = &fetcher.memory_tracker {
            Some(tracker.reserve(uncompressed_bytes)
                .context(crate::error::RecordBatchSnafu)?)
        } else {
            None
        };
        loop {
            match decoder.try_decode().context(ReadParquetSnafu { path: &file_path })? {
                DecodeResult::NeedsData(ranges) => {
                    let data = fetcher.fetch_bytes_with_cache(&ranges).await?;
                    decoder.push_ranges(ranges, data)
                        .context(ReadParquetSnafu { path: &file_path })?;
                }
                DecodeResult::Data(batch) => yield batch,
                DecodeResult::Finished => break,
            }
        }
    }
    .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decoder_memory_projection_lifetime_and_error() {
        use std::sync::Arc;

        use datatypes::arrow::array::{Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field, Schema};
        use object_store::services::Memory;
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ArrowReaderOptions;
        use parquet::file::properties::WriterProperties;

        use crate::sst::parquet::metadata::MetadataLoader;
        use crate::sst::parquet::reader::MetadataCacheMetrics;

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..8)),
                Arc::new(StringArray::from_iter_values(
                    (0..8).map(|i| format!("{i}:{}", "x".repeat(512))),
                )),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let file_size = bytes.len() as u64;
        store.write("memory.parquet", bytes).await.unwrap();
        let metadata = MetadataLoader::new(store.clone(), "memory.parquet", file_size)
            .load(&mut MetadataCacheMetrics::default())
            .await
            .unwrap();
        let sizes: Vec<_> = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.column(0).uncompressed_size() as usize)
            .collect();
        let aligned = |bytes: usize| bytes.div_ceil(1024) * 1024;
        let limit = aligned(*sizes.iter().max().unwrap());
        assert!(metadata.row_group(0).total_byte_size() as usize > limit);
        let projection = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0]);
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new()).unwrap();
        let tracker = QueryMemoryTracker::builder(limit).build();
        let cache = CacheStrategy::EnableAll(Arc::new(
            crate::cache::CacheManager::builder()
                .page_cache_size(1024 * 1024)
                .build(),
        ));
        let region_file_id = RegionFileId::new(
            store_api::storage::RegionId::new(1, 1),
            store_api::storage::FileId::random(),
        );
        let build = |row_group_idx, path: &str, selection| {
            let fetcher = SstParquetRangeFetcher::new(
                region_file_id,
                path.into(),
                store.clone(),
                if path == "memory.parquet" {
                    cache.clone()
                } else {
                    CacheStrategy::Disabled
                },
                None,
            )
            .with_memory_tracker(Some(tracker.clone()));
            build_sst_parquet_record_batch_stream(
                metadata.clone(),
                row_group_idx,
                selection,
                projection.clone(),
                fetcher,
                path.into(),
                1,
            )
            .unwrap()
        };
        // Construction (including an unpolled stream's drop) must not reserve memory.
        drop(build(0, "memory.parquet", None));
        assert_eq!(0, tracker.current());
        for (row_group_idx, size) in sizes.iter().enumerate() {
            let mut first = build(row_group_idx, "memory.parquet", None);
            assert_eq!(1, first.next().await.unwrap().unwrap().num_rows());
            assert_eq!(aligned(*size), tracker.current());
            let mut competing = build(row_group_idx, "memory.parquet", None);
            assert!(competing.next().await.unwrap().is_err());
            assert_eq!(aligned(*size), tracker.current());
            // Reserve before decoding, even when the selection would require no page reads.
            let mut skipped = build(
                row_group_idx,
                "memory.parquet",
                Some(RowSelection::from(vec![
                    parquet::arrow::arrow_reader::RowSelector::skip(4),
                ])),
            );
            assert!(skipped.next().await.unwrap().is_err());
            assert_eq!(aligned(*size), tracker.current());
            while first.next().await.transpose().unwrap().is_some() {}
            // Completed streams may remain alive without holding their quota.
            assert_eq!(0, tracker.current());
        }
        // The second read must acquire quota even when all pages are cached.
        store.delete("memory.parquet").await.unwrap();
        let mut cancelled = build(0, "memory.parquet", None);
        cancelled.next().await.unwrap().unwrap();
        drop(cancelled);
        assert_eq!(0, tracker.current());
        let mut failed = build(0, "missing.parquet", None);
        assert!(failed.next().await.unwrap().is_err());
        drop(failed);
        assert_eq!(0, tracker.current());
        let mut skipped = build(
            0,
            "memory.parquet",
            Some(RowSelection::from(vec![
                parquet::arrow::arrow_reader::RowSelector::skip(4),
            ])),
        );
        assert!(skipped.next().await.is_none());
        assert_eq!(0, tracker.current());
    }
    #[tokio::test]
    async fn test_page_fetch_partial_hits_preserve_request_metrics() {
        use crate::cache::CacheManager;
        use crate::cache::parquet_page::PAGE_SIZE;
        use object_store::services::Memory;
        use std::sync::Arc;
        use store_api::storage::{FileId, RegionId};

        let store = ObjectStore::new(Memory::default()).unwrap();
        let size = PAGE_SIZE * 2 + 10;
        store
            .write(
                "bytes.parquet",
                (0..size).map(|n| n as u8).collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let manager = Arc::new(CacheManager::builder().page_cache_size(1024 * 1024).build());
        let id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let metrics = ParquetFetchMetrics::default();
        let fetcher = SstParquetRangeFetcher::new(
            id,
            "bytes.parquet".into(),
            store.clone(),
            CacheStrategy::EnableAll(manager),
            Some(metrics.clone()),
        )
        .with_file_size(size);
        assert_eq!(
            fetcher.fetch_bytes_with_cache(&[1..5, 1..1]).await.unwrap()[0],
            Bytes::from_static(&[1, 2, 3, 4])
        );
        assert_eq!(
            fetcher.fetch_bytes_with_cache(&[3..7, 1..1]).await.unwrap()[0],
            Bytes::from_static(&[3, 4, 5, 6])
        );
        assert_eq!(
            fetcher
                .fetch_bytes_with_cache(&[PAGE_SIZE - 2..PAGE_SIZE + 2, 1..1])
                .await
                .unwrap()[0],
            Bytes::from_static(&[254, 255, 0, 1])
        );
        {
            let data = metrics.data.lock().unwrap();
            assert_eq!(data.page_cache_hit, 2);
            assert_eq!(data.cache_miss, 2);
            assert_eq!(data.write_cache_hit, 0);
            assert_eq!(data.page_size_needed, 12);
            assert_eq!(data.page_size_to_fetch_mem, 6);
            assert_eq!(data.page_size_to_fetch_store, PAGE_SIZE * 2);
        }
        // Completed pages serve another fetch without contacting the deleted backing object.
        store.delete("bytes.parquet").await.unwrap();
        let result = fetcher
            .fetch_bytes_with_cache(&[PAGE_SIZE + 1..PAGE_SIZE + 3, size..size])
            .await
            .unwrap();
        assert_eq!(&result[0][..], &[1, 2]);
        assert!(result[1].is_empty());
        let disabled = SstParquetRangeFetcher::new(
            id,
            "bytes.parquet".into(),
            store,
            CacheStrategy::Disabled,
            None,
        );
        assert!(
            disabled
                .fetch_bytes_with_cache(&[1..5, 1..1])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_cached_and_disabled_fetch_return_identical_ranges() {
        use crate::cache::CacheManager;
        use crate::cache::parquet_page::PAGE_SIZE;
        use object_store::services::Memory;
        use std::sync::Arc;
        use store_api::storage::{FileId, RegionId};

        let store = ObjectStore::new(Memory::default()).unwrap();
        let size = PAGE_SIZE + 10;
        store
            .write(
                "bytes.parquet",
                (0..size).map(|n| n as u8).collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let id = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let manager = Arc::new(CacheManager::builder().page_cache_size(1024 * 1024).build());
        let cached = SstParquetRangeFetcher::new(
            id,
            "bytes.parquet".into(),
            store.clone(),
            CacheStrategy::EnableAll(manager.clone()),
            None,
        );
        let disabled = SstParquetRangeFetcher::new(
            id,
            "bytes.parquet".into(),
            store.clone(),
            CacheStrategy::Disabled,
            None,
        );
        let compact = SstParquetRangeFetcher::new(
            id,
            "bytes.parquet".into(),
            store,
            CacheStrategy::Compaction(manager),
            None,
        );
        let ranges = vec![size - 3..size, 1..6, PAGE_SIZE - 2..PAGE_SIZE + 3, 4..8];
        let expected = disabled.fetch_bytes_with_cache(&ranges).await.unwrap();
        assert_eq!(
            cached.fetch_bytes_with_cache(&ranges).await.unwrap(),
            expected
        );
        assert_eq!(
            cached.fetch_bytes_with_cache(&ranges).await.unwrap(),
            expected
        );
        assert_eq!(
            compact.fetch_bytes_with_cache(&ranges).await.unwrap(),
            expected
        );
    }
}
