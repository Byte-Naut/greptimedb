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

//! Request-scoped I/O batching beneath a cache's native load deduplication.
//!
//! Each native loader submits a byte request. The driver polls the caller's concurrent
//! loads alongside I/O and may borrow its reader. There is no keyed loading registry,
//! background task, or batching timer. Each queue belongs to one immutable file.

use std::fmt;
use std::future::Future;
use std::io;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::range_read::{RangeReader, ReadPlan};

/// Limits for one request-scoped queue, not a total process memory limit.
/// Requested bytes and planned read buffers are each bounded separately; response
/// copies and cache consumers may coexist with the read buffers.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Queue capacity and maximum requests per batch, not total in-flight requests.
    pub max_requests: usize,
    /// Bounds admitted requested bytes (including active reads) and each batch's
    /// planned read bytes independently. Gap merging is disabled if it exceeds this limit.
    pub max_requested_bytes: u32,
    /// Maximum number of concurrently executing physical reads.
    pub max_concurrent_reads: usize,
    /// Maximum byte length of each physical read.
    pub max_read_size: NonZeroU64,
    /// Maximum gap merged between requested ranges.
    pub max_gap: u64,
}

/// Events for caller-owned metrics. `Released` also runs on cancellation or failure.
/// Queued-byte accounting is requested bytes, not unique allocations or process RSS.
#[derive(Debug, Clone, Copy)]
pub enum BatchEvent {
    /// A request acquired its byte budget.
    Admitted { bytes: u64 },
    /// A request released its byte budget.
    Released { bytes: u64 },
    /// One batch is about to execute. Planned bytes include merged gaps.
    Started {
        requests: usize,
        reads: usize,
        requested_bytes: u64,
        planned_bytes: u64,
        /// Sum of the admitted requests' queue waits.
        queue_wait: Duration,
    },
    /// A batch completed or its driver was cancelled.
    Finished {
        elapsed: Duration,
        failed: bool,
        cancelled: bool,
    },
}

/// Owned submission handle suitable for a `Send + 'static` cache loader.
///
/// Every clone submits to the same immutable file or reading view. File identity
/// is deliberately supplied by ownership of the queue, not by individual requests.
pub struct ReadQueue {
    sender: mpsc::Sender<Request>,
    permits: Arc<Semaphore>,
    max_bytes: u32,
    observer: Observer,
}

impl Clone for ReadQueue {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            permits: self.permits.clone(),
            max_bytes: self.max_bytes,
            observer: self.observer.clone(),
        }
    }
}

impl ReadQueue {
    /// Returns whether this error was produced because the queue's driver closed.
    /// A caller may retry on a new driver; a backend's ordinary `BrokenPipe`
    /// error does not satisfy this predicate.
    pub fn is_closed_error(error: &io::Error) -> bool {
        error
            .get_ref()
            .is_some_and(|source| source.is::<QueueClosed>())
    }

    /// Submits one nonempty unit. The returned bytes own independent storage so a
    /// cached page does not retain an entire merged read buffer.
    pub async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let len = range
            .end
            .checked_sub(range.start)
            .ok_or_else(invalid_request)?;
        if len == 0 || len > u64::from(self.max_bytes) {
            return Err(invalid_request());
        }
        let permit = self
            .permits
            .clone()
            .acquire_many_owned(len as u32)
            .await
            .map_err(|_| closed())?;
        (self.observer)(BatchEvent::Admitted { bytes: len });
        let accounting = RequestAccounting {
            bytes: len,
            observer: self.observer.clone(),
        };
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Request {
                range,
                response,
                admitted: Instant::now(),
                _permit: permit,
                _accounting: accounting,
            })
            .await
            .map_err(|_| closed())?;
        receiver.await.map_err(|_| closed())?
    }
}

/// Drives one caller's concurrent loads and the byte requests selected by the cache.
/// Native cache deduplication may choose loaders from other callers; every caller
/// must drive its own queue until its load future completes or is cancelled.
pub struct BatchReader {
    receiver: mpsc::Receiver<Request>,
    permits: Arc<Semaphore>,
    config: BatchConfig,
    observer: Observer,
}

impl Drop for BatchReader {
    fn drop(&mut self) {
        self.permits.close();
    }
}

impl BatchReader {
    /// Creates a read queue and its driver, in that order, without an event observer.
    /// The caller must poll [`Self::drive`] while awaiting reads submitted to the queue.
    /// Returns [`io::ErrorKind::InvalidInput`] if either admission limit is zero or
    /// exceeds the semaphore's supported capacity, or read concurrency is zero.
    pub fn new(config: BatchConfig) -> io::Result<(ReadQueue, Self)> {
        Self::with_observer(config, |_| {})
    }

    /// The observer must be inexpensive; it runs synchronously on the caller task.
    pub fn with_observer(
        config: BatchConfig,
        observer: impl Fn(BatchEvent) + Send + Sync + 'static,
    ) -> io::Result<(ReadQueue, Self)> {
        if config.max_concurrent_reads == 0
            || config.max_requests == 0
            || config.max_requests > Semaphore::MAX_PERMITS
            || config.max_requested_bytes == 0
            || config.max_requested_bytes as usize > Semaphore::MAX_PERMITS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid batch read queue limits",
            ));
        }
        let observer: Observer = Arc::new(observer);
        let (sender, receiver) = mpsc::channel(config.max_requests);
        let permits = Arc::new(Semaphore::new(config.max_requested_bytes as usize));
        Ok((
            ReadQueue {
                sender,
                permits: permits.clone(),
                max_bytes: config.max_requested_bytes,
                observer: observer.clone(),
            },
            Self {
                receiver,
                permits,
                config,
                observer,
            },
        ))
    }

    /// Polls `loads` concurrently with I/O for one immutable file. Callers should
    /// join load futures rather than await queue submissions sequentially. A queue
    /// may be cloned across requests, but its driver must remain polled until all
    /// those requests complete. Dropping this future closes the queue.
    ///
    /// Calls [`RangeReader::read`] for each planned range to preserve the physical
    /// read-size bound even if a reader's `read_vec` implementation coalesces ranges.
    pub async fn drive<L, R>(mut self, loads: L, reader: &R) -> L::Output
    where
        L: Future,
        R: RangeReader + ?Sized,
    {
        tokio::pin!(loads);
        loop {
            tokio::select! {
                biased;
                result = &mut loads => return result,
                active = self.read_batch(reader) => {
                    if !active {
                        return loads.await;
                    }
                }
            }
        }
    }

    async fn read_batch<R: RangeReader + ?Sized>(&mut self, reader: &R) -> bool {
        let Some(first) = self.receiver.recv().await else {
            return false;
        };
        let mut requests = vec![first];
        while requests.len() < self.config.max_requests {
            match self.receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(_) => break,
            }
        }
        requests.retain(|request| !request.response.is_closed());
        if requests.is_empty() {
            return true;
        }
        let ranges: Vec<_> = requests
            .iter()
            .map(|request| request.range.clone())
            .collect();
        let planned = ReadPlan::new(&ranges, self.config.max_read_size, self.config.max_gap)
            .and_then(|plan| {
                let bytes: u64 = plan
                    .ranges()
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum();
                if bytes > u64::from(self.config.max_requested_bytes) {
                    // Without gaps, the union cannot exceed the admitted requested bytes.
                    ReadPlan::new(&ranges, self.config.max_read_size, 0)
                } else {
                    Ok(plan)
                }
            });
        let plan = match planned {
            Ok(plan) => plan,
            Err(error) => {
                let message = error.to_string();
                let kind = error.kind();
                for request in requests {
                    let _ = request
                        .response
                        .send(Err(io::Error::new(kind, message.clone())));
                }
                return true;
            }
        };
        (self.observer)(BatchEvent::Started {
            requests: requests.len(),
            reads: plan.ranges().len(),
            requested_bytes: ranges.iter().map(|range| range.end - range.start).sum(),
            planned_bytes: plan
                .ranges()
                .iter()
                .map(|range| range.end - range.start)
                .sum(),
            queue_wait: requests
                .iter()
                .map(|request| request.admitted.elapsed())
                .sum(),
        });
        let work = BatchWork {
            started: Instant::now(),
            observer: self.observer.clone(),
            finished: false,
        };
        let result: io::Result<Vec<Bytes>> = futures::stream::iter(plan.ranges().iter().cloned())
            .map(|range| reader.read(range))
            .buffered(self.config.max_concurrent_reads)
            .try_collect()
            .await;
        let valid = result.as_ref().is_ok_and(|buffers| {
            buffers.len() == plan.ranges().len()
                && buffers
                    .iter()
                    .zip(plan.ranges())
                    .all(|(buffer, range)| buffer.len() as u64 == range.end - range.start)
        });
        work.finish(!valid);
        match result {
            Err(error) => {
                let error = Arc::new(error);
                for request in requests {
                    let _ = request
                        .response
                        .send(Err(io::Error::new(error.kind(), error.clone())));
                }
            }
            Ok(buffers) => {
                for request in requests {
                    request.respond(|range| {
                        if valid {
                            Self::copy_response(&plan, &buffers, range)
                        } else {
                            Err(invalid_response())
                        }
                    });
                }
            }
        }
        true
    }

    fn copy_response(plan: &ReadPlan, buffers: &[Bytes], range: Range<u64>) -> io::Result<Bytes> {
        let requested_len = range.end.saturating_sub(range.start);
        let slices = plan.slices(range)?;
        let capacity = usize::try_from(requested_len).map_err(|_| invalid_response())?;
        let mut output = BytesMut::with_capacity(capacity);
        for slice in slices {
            let bytes = buffers
                .get(slice.read_index)
                .and_then(|buffer| buffer.get(slice.range))
                .ok_or_else(invalid_response)?;
            output.extend_from_slice(bytes);
        }
        Ok(output.freeze())
    }
}

type Observer = Arc<dyn Fn(BatchEvent) + Send + Sync>;

struct BatchWork {
    started: Instant,
    observer: Observer,
    finished: bool,
}

impl BatchWork {
    fn finish(mut self, failed: bool) {
        self.finished = true;
        (self.observer)(BatchEvent::Finished {
            elapsed: self.started.elapsed(),
            failed,
            cancelled: false,
        });
    }
}

impl Drop for BatchWork {
    fn drop(&mut self) {
        if !self.finished {
            (self.observer)(BatchEvent::Finished {
                elapsed: self.started.elapsed(),
                failed: false,
                cancelled: true,
            });
        }
    }
}

struct Request {
    range: Range<u64>,
    response: oneshot::Sender<io::Result<Bytes>>,
    admitted: Instant,
    _permit: OwnedSemaphorePermit,
    _accounting: RequestAccounting,
}

impl Request {
    fn respond(self, response: impl FnOnce(Range<u64>) -> io::Result<Bytes>) {
        if !self.response.is_closed() {
            let result = response(self.range);
            let _ = self.response.send(result);
        }
    }
}

struct RequestAccounting {
    bytes: u64,
    observer: Observer,
}

impl Drop for RequestAccounting {
    fn drop(&mut self) {
        (self.observer)(BatchEvent::Released { bytes: self.bytes });
    }
}

#[derive(Debug)]
struct QueueClosed;

impl fmt::Display for QueueClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Read queue driver closed")
    }
}

impl std::error::Error for QueueClosed {}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, QueueClosed)
}

fn invalid_request() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "Read request is empty, reversed or exceeds the queue byte limit",
    )
}

fn invalid_response() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Reader returned unexpected response lengths",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use futures::future;

    use super::*;
    use crate::range_read::Metadata;

    #[derive(Clone, Copy, Default)]
    enum Response {
        #[default]
        Exact,
        Short,
        Long,
        Error,
        Pending,
    }

    #[derive(Default)]
    struct Reader {
        ranges: Mutex<Vec<Range<u64>>>,
        buffers: Mutex<Vec<Bytes>>,
        active: AtomicUsize,
        peak: AtomicUsize,
        response: Response,
        gate: Option<Semaphore>,
    }

    struct ActiveRead<'a>(&'a AtomicUsize);

    impl Drop for ActiveRead<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl RangeReader for Reader {
        async fn metadata(&self) -> io::Result<Metadata> {
            Ok(Metadata {
                content_length: u64::MAX,
            })
        }

        async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
            let _guard = ActiveRead(&self.active);
            self.peak.fetch_max(active, Ordering::Relaxed);
            tokio::task::yield_now().await;
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            match self.response {
                Response::Error => {
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
                }
                Response::Pending => return std::future::pending().await,
                _ => {}
            }
            let mut data: Vec<_> = range.map(|offset| offset as u8).collect();
            match self.response {
                Response::Short => {
                    data.pop();
                }
                Response::Long => data.push(0),
                _ => {}
            }
            let data = Bytes::from(data);
            self.buffers.lock().unwrap().push(data.clone());
            Ok(data)
        }

        async fn read_vec(&self, _: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
            panic!("batch driver must preserve read boundaries")
        }
    }

    fn config(requests: usize, bytes: u32) -> BatchConfig {
        BatchConfig {
            max_requests: requests,
            max_requested_bytes: bytes,
            max_read_size: NonZeroU64::new(8).unwrap(),
            max_concurrent_reads: 2,
            max_gap: 0,
        }
    }

    fn observer() -> (
        Arc<Mutex<Vec<BatchEvent>>>,
        impl Fn(BatchEvent) + Send + Sync + 'static,
    ) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let clone = events.clone();
        (events, move |event| clone.lock().unwrap().push(event))
    }

    fn assert_released(events: &[BatchEvent]) {
        let mut bytes = 0;
        for event in events {
            match event {
                BatchEvent::Admitted { bytes: n } => bytes += *n as i64,
                BatchEvent::Released { bytes: n } => bytes -= *n as i64,
                _ => {}
            }
            assert!(bytes >= 0);
        }
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn test_overlapping_requests_merge_and_detach() {
        let mut config = config(4, 64);
        config.max_read_size = NonZeroU64::new(64).unwrap();
        let (queue, driver) = BatchReader::new(config).unwrap();
        let second = queue.clone();
        let reader = Reader::default();
        let result = driver
            .drive(
                future::join_all([queue.read(0..8), second.read(4..12)]),
                &reader,
            )
            .await;
        assert_eq!(
            reader.ranges.lock().unwrap().as_slice(),
            std::slice::from_ref(&(0..12))
        );
        for (result, range) in result.into_iter().zip([0..8, 4..12]) {
            let result = result.unwrap();
            assert_eq!(&result[..], range.clone().collect::<Vec<u8>>());
            let merged = &reader.buffers.lock().unwrap()[0];
            assert_ne!(
                result.as_ptr(),
                merged.as_ptr().wrapping_add(range.start as usize)
            );
        }
    }

    #[tokio::test]
    async fn test_size_concurrency_and_admission_bounds() {
        let reader = Reader::default();
        let (events, observe) = observer();
        let (queue, driver) = BatchReader::with_observer(config(2, 24), observe).unwrap();
        let results = tokio::time::timeout(
            Duration::from_secs(5),
            driver.drive(
                future::join_all((0..8).map(|n| queue.read(n * 12..n * 12 + 12))),
                &reader,
            ),
        )
        .await
        .unwrap();
        assert!(results.iter().all(Result::is_ok));
        assert!(
            reader
                .ranges
                .lock()
                .unwrap()
                .iter()
                .all(|r| r.end - r.start <= 8)
        );
        assert_eq!(reader.peak.load(Ordering::Relaxed), 2);
        let events = events.lock().unwrap();
        assert_released(&events);
        let mut admitted = 0;
        for event in events.iter() {
            match event {
                BatchEvent::Admitted { bytes } => {
                    admitted += bytes;
                    assert!(admitted <= 24);
                }
                BatchEvent::Released { bytes } => admitted -= bytes,
                BatchEvent::Started {
                    requests,
                    requested_bytes,
                    planned_bytes,
                    ..
                } => {
                    assert!(*requests <= 2);
                    assert!(*requested_bytes <= 24);
                    assert!(*planned_bytes <= 24);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_gap_merge_respects_buffer_budget() {
        let mut config = config(2, 8);
        config.max_read_size = NonZeroU64::new(1024).unwrap();
        config.max_gap = 1024;
        let (queue, driver) = BatchReader::new(config).unwrap();
        let reader = Reader::default();
        let results = driver
            .drive(
                future::join_all([queue.read(0..4), queue.read(100..104)]),
                &reader,
            )
            .await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(*reader.ranges.lock().unwrap(), vec![0..4, 100..104]);
    }

    #[tokio::test]
    async fn test_failed_and_malformed_reads_complete_every_request() {
        for (response, kind) in [
            (Response::Error, io::ErrorKind::PermissionDenied),
            (Response::Short, io::ErrorKind::InvalidData),
            (Response::Long, io::ErrorKind::InvalidData),
        ] {
            let reader = Reader {
                response,
                ..Default::default()
            };
            let (events, observe) = observer();
            let (queue, driver) = BatchReader::with_observer(config(2, 8), observe).unwrap();
            let results = tokio::time::timeout(
                Duration::from_secs(5),
                driver.drive(
                    future::join_all([queue.read(0..4), queue.read(4..8)]),
                    &reader,
                ),
            )
            .await
            .unwrap();
            for result in results {
                assert_eq!(result.unwrap_err().kind(), kind);
            }
            let events = events.lock().unwrap();
            assert_released(&events);
            assert!(events.iter().any(|event| matches!(
                event,
                BatchEvent::Finished {
                    failed: true,
                    cancelled: false,
                    ..
                }
            )));
        }
    }

    #[tokio::test]
    async fn test_dropping_driver_wakes_queued_and_budget_blocked_reads() {
        let (events, observe) = observer();
        let (queue, driver) = BatchReader::with_observer(config(1, 4), observe).unwrap();
        let mut queued = Box::pin(queue.read(0..4));
        let mut blocked = Box::pin(queue.read(4..8));
        assert!(futures::poll!(&mut queued).is_pending());
        assert!(futures::poll!(&mut blocked).is_pending());
        drop(driver);
        assert_eq!(queued.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(blocked.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_released(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn test_cancelled_driver_drops_physical_reads_and_releases_budget() {
        let reader = Reader {
            response: Response::Pending,
            ..Default::default()
        };
        let (events, observe) = observer();
        let (queue, driver) = BatchReader::with_observer(config(2, 8), observe).unwrap();
        let mut work = Box::pin(driver.drive(queue.read(0..8), &reader));
        assert!(futures::poll!(&mut work).is_pending());
        assert_eq!(reader.active.load(Ordering::Relaxed), 1);
        drop(work);
        assert_eq!(reader.active.load(Ordering::Relaxed), 0);
        assert_eq!(
            queue.read(0..8).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        let events = events.lock().unwrap();
        assert_released(&events);
        assert!(events.iter().any(|event| matches!(
            event,
            BatchEvent::Finished {
                cancelled: true,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn test_cancelled_submission_is_not_read() {
        let reader = Reader::default();
        let (events, observe) = observer();
        let (queue, driver) = BatchReader::with_observer(config(2, 8), observe).unwrap();
        let mut cancelled = Box::pin(queue.read(0..4));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        let result = driver.drive(queue.read(4..8), &reader).await.unwrap();
        assert_eq!(&result[..], &[4, 5, 6, 7]);
        assert_eq!(
            reader.ranges.lock().unwrap().as_slice(),
            std::slice::from_ref(&(4..8))
        );
        assert_released(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn test_cancelling_one_submission_during_read_preserves_other_response() {
        let reader = Reader {
            gate: Some(Semaphore::new(0)),
            ..Default::default()
        };
        let (events, observe) = observer();
        let (queue, driver) = BatchReader::with_observer(config(2, 8), observe).unwrap();
        let mut cancelled = Box::pin(queue.read(0..4));
        assert!(futures::poll!(&mut cancelled).is_pending());
        let mut work = Box::pin(driver.drive(queue.read(4..8), &reader));
        assert!(futures::poll!(&mut work).is_pending());
        assert_eq!(reader.active.load(Ordering::Relaxed), 1);

        drop(cancelled);
        reader.gate.as_ref().unwrap().add_permits(1);
        let result = tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&result[..], &[4, 5, 6, 7]);
        assert_eq!(
            reader.ranges.lock().unwrap().as_slice(),
            std::slice::from_ref(&(0..8))
        );
        assert_released(&events.lock().unwrap());
    }

    #[test]
    fn test_cancelled_response_does_not_construct_payload() {
        let (events, observe) = observer();
        let observer: Observer = Arc::new(observe);
        let permits = Arc::new(Semaphore::new(4));
        let (response, receiver) = oneshot::channel();
        let request = Request {
            range: 0..4,
            response,
            admitted: Instant::now(),
            _permit: permits.clone().try_acquire_many_owned(4).unwrap(),
            _accounting: RequestAccounting {
                bytes: 4,
                observer: observer.clone(),
            },
        };
        observer(BatchEvent::Admitted { bytes: 4 });
        drop(receiver);
        request.respond(|_| panic!("Cancelled response must not allocate or copy a payload"));
        assert_eq!(permits.available_permits(), 4);
        assert_released(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn test_loader_failure_closes_driver() {
        let reader = Reader::default();
        let (queue, driver) = BatchReader::new(config(1, 4)).unwrap();
        let result = driver
            .drive(std::future::ready(Err::<(), _>("loader failed")), &reader)
            .await;
        assert_eq!(result, Err("loader failed"));
        assert!(reader.ranges.lock().unwrap().is_empty());
        assert_eq!(
            queue.read(0..4).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn test_queue_closure_is_distinct_from_backend_broken_pipe() {
        assert!(ReadQueue::is_closed_error(&closed()));
        assert!(!ReadQueue::is_closed_error(&io::Error::new(
            io::ErrorKind::BrokenPipe,
            "backend disconnected"
        )));
    }

    #[tokio::test]
    async fn test_invalid_requests_and_configs() {
        let (queue, _driver) = BatchReader::new(config(1, 4)).unwrap();
        for range in [0..0, 0..5, Range { start: 8, end: 4 }] {
            assert_eq!(
                queue.read(range).await.unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let mut no_concurrency = config(1, 4);
        no_concurrency.max_concurrent_reads = 0;
        for config in [config(0, 4), config(1, 0), no_concurrency] {
            assert!(BatchReader::new(config).is_err());
        }
    }
}
