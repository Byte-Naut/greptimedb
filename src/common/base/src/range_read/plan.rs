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

//! Pure byte-range planning, independent of caches and I/O execution.
//!
//! Planned bytes include merged gaps. Physical readers may retry or merge reads
//! further, so actual I/O must be measured at the reader.

use std::io;
use std::num::NonZeroU64;
use std::ops::Range;

/// A portion of one read response needed to reconstruct a requested range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSlice {
    /// Index into [`ReadPlan::ranges`] and the corresponding response buffers.
    pub read_index: usize,
    /// Byte offsets within that response buffer.
    pub range: Range<usize>,
}

/// Sorted, non-overlapping reads with a hard limit on each read's size.
#[derive(Debug, Clone)]
pub struct ReadPlan {
    ranges: Vec<Range<u64>>,
}

impl ReadPlan {
    /// Plans reads for arbitrary byte ranges from the same immutable file.
    ///
    /// Empty ranges are ignored and reversed ranges are rejected. Overlapping
    /// ranges are read once. Neighboring ranges may be joined across at most
    /// `max_gap` bytes, provided the resulting read fits `max_read_size`.
    /// A single larger input range is split to respect that limit as well.
    /// The caller supplies file ranges already clipped to the file size.
    /// This plan bounds each read, not the total number or size of reads. Admission
    /// and concurrency limits belong to the reader that executes the plan.
    pub fn new(ranges: &[Range<u64>], max_read_size: NonZeroU64, max_gap: u64) -> io::Result<Self> {
        let mut sorted = Vec::with_capacity(ranges.len());
        for range in ranges {
            validate_range(range)?;
            if !range.is_empty() {
                sorted.push(range.clone());
            }
        }
        sorted.sort_unstable_by_key(|range| (range.start, range.end));

        // First build the union, so splitting a large range does not cause a
        // later overlapping request to read the same bytes again.
        let mut union: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
        for range in sorted {
            if let Some(previous) = union.last_mut()
                && range.start <= previous.end
            {
                previous.end = previous.end.max(range.end);
                continue;
            }
            union.push(range);
        }

        let mut reads: Vec<Range<u64>> = Vec::new();
        let max_size = max_read_size.get();
        for range in union {
            if let Some(previous) = reads.last_mut()
                && range.start - previous.end <= max_gap
                && range.end - previous.start <= max_size
            {
                previous.end = range.end;
                continue;
            }
            let mut start = range.start;
            while start < range.end {
                let end = start + (range.end - start).min(max_size);
                reads.push(start..end);
                start = end;
            }
        }
        Ok(Self { ranges: reads })
    }

    /// Returns the file ranges to read, in increasing byte order.
    pub fn ranges(&self) -> &[Range<u64>] {
        &self.ranges
    }

    /// Maps a request to ordered slices of the planned response buffers.
    ///
    /// Empty requests return no slices, even outside planned ranges.
    /// Every requested byte must be covered. Requests spanning an unread gap
    /// return an error rather than silently omitting bytes. Actual response
    /// lengths still need validation by the reader before indexing buffers.
    pub fn slices(&self, request: Range<u64>) -> io::Result<Vec<ReadSlice>> {
        validate_range(&request)?;
        let mut cursor = request.start;
        let mut slices = Vec::new();
        let first = self.ranges.partition_point(|range| range.end <= cursor);
        for (read_index, range) in self.ranges.iter().enumerate().skip(first) {
            if cursor == request.end {
                break;
            }
            if range.start > cursor {
                return Err(invalid_input("Request contains an unplanned byte range"));
            }
            let end = range.end.min(request.end);
            slices.push(ReadSlice {
                read_index,
                range: to_usize(cursor - range.start)?..to_usize(end - range.start)?,
            });
            cursor = end;
        }
        if cursor != request.end {
            return Err(invalid_input("Request extends beyond planned reads"));
        }
        Ok(slices)
    }
}

fn validate_range(range: &Range<u64>) -> io::Result<()> {
    if range.start > range.end {
        return Err(invalid_input("Byte range start exceeds end"));
    }
    Ok(())
}

fn to_usize(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_input("Byte offset exceeds addressable memory"))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn assert_reconstructs(plan: &ReadPlan, request: Range<u64>) {
        let actual: Vec<_> = plan
            .slices(request.clone())
            .unwrap()
            .into_iter()
            .flat_map(|slice| {
                let read = &plan.ranges()[slice.read_index];
                assert!(slice.range.start < slice.range.end);
                assert!(slice.range.end as u64 <= read.end - read.start);
                (read.start + slice.range.start as u64)..(read.start + slice.range.end as u64)
            })
            .collect();
        assert_eq!(actual, request.collect::<Vec<_>>());
    }

    #[test]
    fn test_merge_gap_and_size_limits() {
        let inputs = [16..24, 0..8, 4..8, 8..8, 32..48];
        let plan = ReadPlan::new(&inputs, nonzero(24), 8).unwrap();
        assert_eq!(plan.ranges(), &[0..24, 32..48]);
        assert_eq!(
            plan.slices(4..20).unwrap(),
            vec![ReadSlice {
                read_index: 0,
                range: 4..20
            }]
        );
        for input in inputs {
            assert_reconstructs(&plan, input);
        }
        // Crossing a merged gap is allowed, but crossing an unread gap is not.
        assert_reconstructs(&plan, 7..17);
        assert!(plan.slices(20..36).is_err());
        assert_eq!(
            ReadPlan::new(&[0..8, 16..24], nonzero(24), 7)
                .unwrap()
                .ranges(),
            &[0..8, 16..24]
        );
        assert_eq!(
            ReadPlan::new(&[0..8, 16..24], nonzero(23), 8)
                .unwrap()
                .ranges(),
            &[0..8, 16..24]
        );
    }

    #[test]
    fn test_split_overlaps_without_duplicate_reads() {
        let inputs = [18..30, 7..18, 0..25, 0..25, 2..4];
        let plan = ReadPlan::new(&inputs, nonzero(8), 0).unwrap();
        assert_eq!(plan.ranges(), &[0..8, 8..16, 16..24, 24..30]);
        for input in inputs {
            assert_reconstructs(&plan, input);
        }
        assert_eq!(
            plan.slices(7..18).unwrap(),
            vec![
                ReadSlice {
                    read_index: 0,
                    range: 7..8
                },
                ReadSlice {
                    read_index: 1,
                    range: 0..8
                },
                ReadSlice {
                    read_index: 2,
                    range: 0..2
                },
            ]
        );
    }

    #[test]
    fn test_empty_and_invalid_ranges() {
        for inputs in [vec![], vec![0..0, 4..4, u64::MAX..u64::MAX]] {
            let plan = ReadPlan::new(&inputs, nonzero(8), 0).unwrap();
            assert!(plan.ranges().is_empty());
            assert!(plan.slices(100..100).unwrap().is_empty());
            assert!(plan.slices(0..1).is_err());
        }
        let reversed = Range { start: 8, end: 4 };
        assert_eq!(
            ReadPlan::new(std::slice::from_ref(&reversed), nonzero(8), 0)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        let plan = ReadPlan::new(&[4..8, 12..16], nonzero(8), 0).unwrap();
        for request in [reversed, 3..5, 7..13, 16..17, 100..101] {
            assert_eq!(
                plan.slices(request).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        for offset in [0, 4, 8, 10, 12, 16, 100, u64::MAX] {
            assert!(plan.slices(offset..offset).unwrap().is_empty());
        }
    }

    #[test]
    fn test_read_plan_at_u64_boundary() {
        let plan = ReadPlan::new(
            std::slice::from_ref(&(u64::MAX - 10..u64::MAX)),
            nonzero(6),
            u64::MAX,
        )
        .unwrap();
        assert_eq!(
            plan.ranges(),
            &[u64::MAX - 10..u64::MAX - 4, u64::MAX - 4..u64::MAX]
        );
        assert_eq!(
            plan.slices(u64::MAX - 2..u64::MAX).unwrap(),
            vec![ReadSlice {
                read_index: 1,
                range: 2..4
            }]
        );
        assert_reconstructs(&plan, u64::MAX - 10..u64::MAX);
        let plan =
            ReadPlan::new(&[0..1, u64::MAX - 1..u64::MAX], nonzero(u64::MAX), u64::MAX).unwrap();
        assert_eq!(plan.ranges(), std::slice::from_ref(&(0..u64::MAX)));
    }

    #[test]
    fn test_all_small_range_pairs() {
        // Check union coverage and reconstruction independently of the planner's
        // sort/merge algorithm, including every ordering and empty endpoint.
        for first_start in 0..=8 {
            for first_end in first_start..=8 {
                for second_start in 0..=8 {
                    for second_end in second_start..=8 {
                        let inputs = [first_start..first_end, second_start..second_end];
                        for max_size in 1..=8 {
                            for max_gap in [0, 1, 8] {
                                let plan =
                                    ReadPlan::new(&inputs, nonzero(max_size), max_gap).unwrap();
                                assert!(
                                    plan.ranges()
                                        .iter()
                                        .all(|r| r.start < r.end && r.end - r.start <= max_size)
                                );
                                assert!(
                                    plan.ranges()
                                        .windows(2)
                                        .all(|pair| pair[0].end <= pair[1].start)
                                );
                                for input in &inputs {
                                    assert_reconstructs(&plan, input.clone());
                                }
                                for byte in 0..8 {
                                    let requested = inputs.iter().any(|r| r.contains(&byte));
                                    let planned = plan.ranges().iter().any(|r| r.contains(&byte));
                                    assert!(!requested || planned);
                                    if max_gap == 0 {
                                        assert_eq!(requested, planned);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
