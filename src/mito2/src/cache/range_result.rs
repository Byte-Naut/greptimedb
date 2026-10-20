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

use std::future::Future;
use std::sync::Arc;

use common_cache::Cache;
use common_cache::accounting::CacheAccounting;

use crate::cache::RANGE_RESULT_TYPE;
use crate::error::Error;
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION};
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};

/// A budget fallback is not cached or shared as a stream. Each caller retains
/// ownership of its own reader; only completed scan results can be shared.
#[derive(Debug)]
pub(crate) enum RangeLoadError {
    Fallback,
    Failed(Arc<Error>),
}

impl From<Error> for RangeLoadError {
    fn from(error: Error) -> Self {
        Self::Failed(Arc::new(error))
    }
}

/// Completed range results and native same-key load deduplication.
pub(crate) struct RangeResultCache {
    cache: Cache<RangeScanCacheKey, Arc<CachedRangeResult>>,
}

impl RangeResultCache {
    /// Creates a cache whose capacity is measured by estimated key and result bytes.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            cache: Cache::new(
                "range_result",
                capacity,
                |key: &RangeScanCacheKey, result: &Arc<CachedRangeResult>| {
                    result_weight(key, &result.value)
                },
                |_, result, cause| {
                    result.accounting.release();
                    CACHE_EVICTION
                        .with_label_values(&[RANGE_RESULT_TYPE, cause.as_str()])
                        .inc();
                },
            )
            .with_metrics(crate::metrics::cache_loading_metrics(RANGE_RESULT_TYPE)),
        }
    }

    /// Looks up a completed result without starting a load.
    pub(crate) async fn get(&self, key: &RangeScanCacheKey) -> Option<Arc<RangeScanCacheValue>> {
        self.cache.get(key).await.map(|result| result.value.clone())
    }

    /// Submits a completed result to the cache's admission and eviction policy.
    #[cfg(test)]
    pub(crate) async fn insert(&self, key: RangeScanCacheKey, value: Arc<RangeScanCacheValue>) {
        let result = CachedRangeResult::new(&key, value);
        self.cache.insert(key, result).await;
    }

    /// Shares completed results using [`Cache::try_get_with`].
    /// [`RangeLoadError::Fallback`] is not cached; each caller must continue its own
    /// stream because partially collected streams cannot be shared.
    pub(crate) async fn get_or_load<F>(
        &self,
        key: &RangeScanCacheKey,
        load: F,
    ) -> Result<Arc<RangeScanCacheValue>, Arc<RangeLoadError>>
    where
        F: Future<Output = Result<Arc<RangeScanCacheValue>, RangeLoadError>>,
    {
        self.cache
            .try_get_with(key, async {
                let value = load.await?;
                Ok(CachedRangeResult::new(key, value))
            })
            .await
            .map(|result| result.value.clone())
    }
}

struct CachedRangeResult {
    value: Arc<RangeScanCacheValue>,
    accounting: CacheAccounting,
}

impl CachedRangeResult {
    fn new(key: &RangeScanCacheKey, value: Arc<RangeScanCacheValue>) -> Arc<Self> {
        let weight = result_weight(key, &value);
        Arc::new(Self {
            value,
            accounting: CacheAccounting::new(
                weight,
                CACHE_BYTES.with_label_values(&[RANGE_RESULT_TYPE]),
            ),
        })
    }
}

fn result_weight(key: &RangeScanCacheKey, value: &RangeScanCacheValue) -> u32 {
    u32::try_from(key.estimated_size().saturating_add(value.estimated_size())).unwrap_or(u32::MAX)
}
