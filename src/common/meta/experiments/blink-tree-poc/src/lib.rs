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

//! Executable, test-only protocol experiment; see the adjacent README for scope.
#![cfg(test)]

mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use opendal::{Error, ErrorKind, Operator, Result, services};
    use serde::{Deserialize, Serialize, de::DeserializeOwned};
    use tokio::sync::{Barrier, Mutex};

    #[derive(Serialize, Deserialize)]
    struct Envelope<T> {
        version: u64,
        value: T,
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Before,
        After,
    }

    /// Models If-Match with per-object serialization: Memory lacks native ETags.
    /// Every client shares this wrapper; each write stores a fresh version.
    struct CasStore {
        op: Operator,
        locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
        fault: Mutex<Option<(String, Fault)>>,
    }

    impl CasStore {
        async fn read<T: DeserializeOwned>(&self, path: &str) -> Result<(u64, T)> {
            let bytes = self.op.read(path).await?.to_vec();
            let e: Envelope<T> = serde_json::from_slice(&bytes).map_err(invalid)?;
            Ok((e.version, e.value))
        }

        async fn if_not_exists<T: Serialize>(&self, path: &str, value: &T) -> Result<()> {
            self.write(path, None, value).await
        }

        async fn if_match<T: Serialize>(&self, path: &str, version: u64, value: &T) -> Result<()> {
            self.write(path, Some(version), value).await
        }

        async fn write<T: Serialize>(&self, path: &str, old: Option<u64>, value: &T) -> Result<()> {
            let lock = {
                let mut locks = self.locks.lock().await;
                locks.entry(path.into()).or_default().clone()
            };
            let _guard = lock.lock().await;
            let fault = {
                let mut f = self.fault.lock().await;
                if f.as_ref().is_some_and(|(p, _)| p == path) {
                    f.take()
                } else {
                    None
                }
            };
            if matches!(fault, Some((_, Fault::Before))) {
                return Err(invalid("injected failure before write"));
            }
            if let Some(version) = old {
                let (actual, _): (_, serde_json::Value) = self.read(path).await?;
                if actual != version {
                    return Err(conflict());
                }
            }
            let bytes = serde_json::to_vec(&Envelope {
                version: old.unwrap_or(0) + 1,
                value,
            })
            .map_err(invalid)?;
            if old.is_none() {
                self.op.write_with(path, bytes).if_not_exists(true).await?;
            } else {
                self.op.write(path, bytes).await?;
            }
            if matches!(fault, Some((_, Fault::After))) {
                return Err(invalid("write applied; response lost"));
            }
            Ok(())
        }

        async fn inject(&self, path: &str, fault: Fault) {
            *self.fault.lock().await = Some((path.into(), fault));
        }
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Entry {
        committed: Option<Vec<u8>>,
        pending: Option<PendingIntent>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct PendingIntent {
        txn_id: String,
        epoch: u64,
        next_value: Vec<u8>,
    }

    /// A fixed-height tree: the root has children, and leaves have entries.
    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Node {
        high_key: Option<u8>,
        right_sibling: Option<String>,
        entries: BTreeMap<u8, Entry>,
        children: BTreeMap<u8, String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Decision {
        committed: bool,
        epoch: u64,
    }

    struct Transaction {
        id: String,
        epoch: u64,
        writes: Vec<(u8, Vec<u8>)>,
    }

    #[derive(Clone)]
    struct Tree {
        store: Arc<CasStore>,
    }

    impl Tree {
        async fn locate(&self, key: u8) -> Result<String> {
            let (_, root): (_, Node) = self.store.read("nodes/root").await?;
            let (_, child) = root
                .children
                .range(..=key)
                .next_back()
                .ok_or_else(conflict)?;
            let mut path = child.clone();
            loop {
                let (_, node): (_, Node) = self.store.read(&path).await?;
                if node.high_key.is_none_or(|high| key < high) {
                    return Ok(path);
                }
                self.repair_parent(&node).await?;
                path = node.right_sibling.ok_or_else(conflict)?;
            }
        }

        async fn repair_parent(&self, node: &Node) -> Result<()> {
            let (Some(high), Some(right)) = (node.high_key, &node.right_sibling) else {
                return Ok(());
            };
            loop {
                let (version, mut root): (_, Node) = self.store.read("nodes/root").await?;
                if root.children.get(&high) == Some(right) {
                    return Ok(());
                }
                root.children.insert(high, right.clone());
                match self.store.if_match("nodes/root", version, &root).await {
                    Err(e) if e.kind() == ErrorKind::ConditionNotMatch => continue,
                    result => return result,
                }
            }
        }

        /// Stage 1 creates an unreachable right copy bound to the left version.
        async fn split_right(&self, path: &str, pivot: u8, right: &str) -> Result<(u64, Node)> {
            let (version, mut left): (_, Node) = self.store.read(path).await?;
            let entries = left.entries.split_off(&pivot);
            if left.entries.is_empty() || entries.is_empty() {
                return Err(conflict());
            }
            let sibling = Node {
                entries,
                high_key: left.high_key,
                right_sibling: left.right_sibling,
                ..Node::default()
            };
            self.store.if_not_exists(right, &sibling).await?;
            left.high_key = Some(pivot);
            left.right_sibling = Some(right.into());
            Ok((version, left))
        }

        async fn split(&self, path: &str, pivot: u8, right: &str) -> Result<()> {
            let (version, left) = self.split_right(path, pivot, right).await?;
            // Stage 2 publishes both the truncation and link in one object CAS.
            self.store.if_match(path, version, &left).await?;
            // Stage 3 is optional for searchability and can be retried by readers.
            self.repair_parent(&left).await
        }

        /// The immutable terminal object arbitrates every commit/abort race.
        async fn decide(&self, id: &str, decision: Decision) -> Result<Decision> {
            let path = format!("txns/{id}");
            match self.store.if_not_exists(&path, &decision).await {
                Ok(()) => Ok(decision),
                Err(e)
                    if e.kind() == ErrorKind::ConditionNotMatch
                        || e.kind() == ErrorKind::Unexpected =>
                {
                    // Includes a lost create response; absence remains an error.
                    self.store.read(&path).await.map(|(_, d)| d)
                }
                Err(e) => Err(e),
            }
        }

        async fn prepare(&self, txn: &Transaction, count: usize) -> Result<()> {
            let status = format!("txns/{}", txn.id);
            for (key, value) in txn.writes.iter().take(count) {
                loop {
                    match self.store.read::<Decision>(&status).await {
                        Err(e) if e.kind() == ErrorKind::NotFound => (),
                        Err(e) => return Err(e),
                        Ok(_) => return Err(conflict()),
                    }
                    let path = self.locate(*key).await?;
                    let (version, mut node): (_, Node) = self.store.read(&path).await?;
                    if node.high_key.is_some_and(|high| *key >= high) {
                        continue;
                    }
                    let entry = node.entries.entry(*key).or_default();
                    let intent = PendingIntent {
                        txn_id: txn.id.clone(),
                        epoch: txn.epoch,
                        next_value: value.clone(),
                    };
                    if entry.pending.as_ref().is_some_and(|p| p != &intent) {
                        return Err(conflict());
                    }
                    entry.pending = Some(intent);
                    match self.store.if_match(&path, version, &node).await {
                        Err(e) if e.kind() == ErrorKind::ConditionNotMatch => continue,
                        result => {
                            result?;
                            break;
                        }
                    }
                }
            }
            Ok(())
        }

        async fn commit(&self, txn: &Transaction) -> Result<()> {
            let desired = decision(true, txn.epoch);
            match self
                .store
                .read::<Decision>(&format!("txns/{}", txn.id))
                .await
            {
                Ok((_, d)) => {
                    return if d == desired {
                        Ok(())
                    } else {
                        Err(conflict())
                    };
                }
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
            for (key, value) in &txn.writes {
                let path = self.locate(*key).await?;
                let (_, node): (_, Node) = self.store.read(&path).await?;
                let expected = PendingIntent {
                    txn_id: txn.id.clone(),
                    epoch: txn.epoch,
                    next_value: value.clone(),
                };
                if node.entries.get(key).and_then(|e| e.pending.as_ref()) != Some(&expected) {
                    return Err(conflict());
                }
            }
            if self.decide(&txn.id, desired.clone()).await? == desired {
                Ok(())
            } else {
                Err(conflict())
            }
        }

        async fn get(&self, key: u8) -> Result<Option<Vec<u8>>> {
            loop {
                let path = self.locate(key).await?;
                let (version, mut node): (_, Node) = self.store.read(&path).await?;
                if node.high_key.is_some_and(|high| key >= high) {
                    continue;
                }
                let Some(entry) = node.entries.get_mut(&key) else {
                    return Ok(None);
                };
                let Some(intent) = entry.pending.take() else {
                    return Ok(entry.committed.clone());
                };
                let decision = self
                    .decide(&intent.txn_id, decision(false, intent.epoch))
                    .await?;
                if decision.committed {
                    if decision.epoch != intent.epoch {
                        return Err(conflict());
                    }
                    entry.committed = Some(intent.next_value);
                }
                let value = entry.committed.clone();
                match self.store.if_match(&path, version, &node).await {
                    Err(e) if e.kind() == ErrorKind::ConditionNotMatch => continue,
                    result => {
                        result?;
                        return Ok(value);
                    }
                }
            }
        }

        async fn range(&self, start: u8, end: u8) -> Result<Vec<(u8, Vec<u8>)>> {
            self.scan(start, end, None).await
        }

        /// A validated leaf image prevents a scan from straddling a commit.
        async fn scan(
            &self,
            start: u8,
            end: u8,
            pause: Option<&(Arc<Barrier>, Arc<Barrier>)>,
        ) -> Result<Vec<(u8, Vec<u8>)>> {
            let mut paused = false;
            loop {
                let mut path = Some(self.locate(start).await?);
                let mut versions = Vec::new();
                let mut values = BTreeMap::new();
                while let Some(current) = path {
                    let (version, node): (_, Node) = self.store.read(&current).await?;
                    self.repair_parent(&node).await?;
                    for key in node.entries.range(start..end).map(|(k, _)| *k) {
                        if let Some(value) = self.get(key).await? {
                            values.insert(key, value);
                        }
                    }
                    versions.push((current, version));
                    if let Some((entered, resume)) = pause.filter(|_| !paused) {
                        paused = true;
                        entered.wait().await;
                        resume.wait().await;
                    }
                    path = node.right_sibling;
                    if node.high_key.is_none_or(|high| high >= end) {
                        break;
                    }
                }
                let mut stable = true;
                for (path, version) in versions {
                    stable &= self.store.read::<Node>(&path).await?.0 == version;
                }
                if stable {
                    return Ok(values.into_iter().collect());
                }
            }
        }
    }

    fn decision(committed: bool, epoch: u64) -> Decision {
        Decision { committed, epoch }
    }

    fn invalid(message: impl std::fmt::Display) -> Error {
        Error::new(ErrorKind::Unexpected, message.to_string())
    }

    fn conflict() -> Error {
        Error::new(
            ErrorKind::ConditionNotMatch,
            "CAS conflict or terminal transaction",
        )
    }

    async fn fixture(split: bool) -> Tree {
        let store = Arc::new(CasStore {
            op: Operator::new(services::Memory::default()).unwrap(),
            locks: Mutex::default(),
            fault: Mutex::default(),
        });
        let tree = Tree { store };
        let mut leaf = Node::default();
        for key in 1..=4 {
            leaf.entries.entry(key).or_default().committed = Some(vec![key]);
        }
        tree.store.if_not_exists("nodes/left", &leaf).await.unwrap();
        let root = Node {
            children: [(0, "nodes/left".into())].into(),
            ..Node::default()
        };
        tree.store.if_not_exists("nodes/root", &root).await.unwrap();
        if split {
            tree.split("nodes/left", 3, "nodes/right").await.unwrap();
        }
        tree
    }

    fn transaction(id: &str, epoch: u64) -> Transaction {
        Transaction {
            id: id.into(),
            epoch,
            writes: vec![(1, vec![11]), (4, vec![14])],
        }
    }

    #[tokio::test]
    async fn cas_conflict_and_unknown_outcome() {
        let t = fixture(false).await;
        assert!(!t.store.op.info().capability().write_with_if_match);
        assert!(t.store.op.info().capability().write_with_if_not_exists);
        t.store.if_not_exists("cas", &1).await.unwrap();
        assert!(t.store.if_not_exists("cas", &2).await.is_err());
        let (a, b) = tokio::join!(
            t.store.if_match("cas", 1, &2),
            t.store.if_match("cas", 1, &3)
        );
        assert_ne!(a.is_ok(), b.is_ok());
        t.store.if_match("cas", 2, &1).await.unwrap();
        assert!(t.store.if_match("cas", 1, &9).await.is_err());
        t.store.inject("cas", Fault::Before).await;
        assert!(t.store.if_match("cas", 3, &4).await.is_err());
        assert_eq!(t.store.read::<u8>("cas").await.unwrap(), (3, 1));
        t.store.inject("cas", Fault::After).await;
        assert!(t.store.if_match("cas", 3, &4).await.is_err());
        assert_eq!(t.store.read::<u8>("cas").await.unwrap(), (4, 4));
    }

    #[tokio::test]
    async fn split_crash_keeps_searchable() {
        for scan in [false, true] {
            let t = fixture(false).await;
            t.store.inject("nodes/left", Fault::After).await;
            assert!(t.split("nodes/left", 3, "nodes/right").await.is_err());
            let (_, root): (_, Node) = t.store.read("nodes/root").await.unwrap();
            assert_eq!(root.children.len(), 1);
            let reader = Tree {
                store: t.store.clone(),
            };
            if scan {
                assert_eq!(
                    reader.range(1, 5).await.unwrap(),
                    (1..=4).map(|k| (k, vec![k])).collect::<Vec<_>>()
                );
            } else {
                for key in 1..=4 {
                    assert_eq!(reader.get(key).await.unwrap(), Some(vec![key]));
                }
            }
            let (_, root): (_, Node) = reader.store.read("nodes/root").await.unwrap();
            assert_eq!(root.children.get(&3).unwrap(), "nodes/right");
            assert_eq!(
                reader.range(2, 4).await.unwrap(),
                vec![(2, vec![2]), (3, vec![3])]
            );
        }
    }

    #[tokio::test]
    async fn split_conflict_preserves_writes() {
        let t = fixture(false).await;
        let (version, stale) = t
            .split_right("nodes/left", 3, "nodes/orphan")
            .await
            .unwrap();
        let txn = transaction("during-split", 1);
        t.prepare(&txn, 2).await.unwrap();
        t.commit(&txn).await.unwrap();
        t.get(1).await.unwrap();
        assert!(
            t.store
                .if_match("nodes/left", version, &stale)
                .await
                .is_err()
        );
        assert_eq!(t.get(4).await.unwrap(), Some(vec![14]));
        assert_eq!(t.range(1, 5).await.unwrap().len(), 4);
        let (_, left): (_, Node) = t.store.read("nodes/left").await.unwrap();
        assert!(left.right_sibling.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_prepare_reader_aborts() {
        let t = fixture(true).await;
        let ready = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let writer = tokio::spawn({
            let (t, ready, resume) = (t.clone(), ready.clone(), resume.clone());
            async move {
                let txn = transaction("partial", 1);
                t.prepare(&txn, 1).await?;
                ready.wait().await;
                resume.wait().await;
                t.prepare(&txn, 2).await?;
                t.commit(&txn).await
            }
        });
        ready.wait().await;
        let (a, b) = tokio::join!(t.get(1), t.get(4));
        assert_eq!((a.unwrap(), b.unwrap()), (Some(vec![1]), Some(vec![4])));
        resume.wait().await;
        assert!(writer.await.unwrap().is_err());
        let (_, d): (_, Decision) = t.store.read("txns/partial").await.unwrap();
        assert!(!d.committed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_reader_repairs() {
        let t = fixture(true).await;
        let txn = transaction("committed", 1);
        t.prepare(&txn, 2).await.unwrap();
        t.store.inject("txns/committed", Fault::After).await;
        t.commit(&txn).await.unwrap();
        assert_eq!(t.get(1).await.unwrap(), Some(vec![11]));
        let (a, b) = tokio::join!(t.get(1), t.get(4));
        assert_eq!((a.unwrap(), b.unwrap()), (Some(vec![11]), Some(vec![14])));
        t.commit(&txn).await.unwrap();
        for path in ["nodes/left", "nodes/right"] {
            let (_, node): (_, Node) = t.store.read(path).await.unwrap();
            assert!(node.entries.values().all(|e| e.pending.is_none()));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_commit_race_has_one_winner() {
        for _ in 0..16 {
            let t = fixture(true).await;
            let txn = transaction("race", 1);
            t.prepare(&txn, 2).await.unwrap();
            let abort = tokio::spawn({
                let t = t.clone();
                async move { t.decide("race", decision(false, 2)).await }
            });
            let commit = t.commit(&txn).await;
            let state = abort.await.unwrap().unwrap();
            assert_eq!(commit.is_ok(), state.committed);
            let values = t.range(1, 5).await.unwrap();
            assert_eq!(values[0].1, vec![if state.committed { 11 } else { 1 }]);
            assert_eq!(values[3].1, vec![if state.committed { 14 } else { 4 }]);
        }
    }

    #[tokio::test]
    async fn new_epoch_fences_old_leader() {
        let t = fixture(true).await;
        let old = transaction("old-epoch", 1);
        t.prepare(&old, 1).await.unwrap();
        let fence = decision(false, 2);
        assert_eq!(t.decide(&old.id, fence.clone()).await.unwrap(), fence);
        assert!(t.prepare(&old, 2).await.is_err());
        assert!(t.commit(&old).await.is_err());
        assert_eq!(t.get(1).await.unwrap(), Some(vec![1]));
        let new = transaction("new-epoch", 2);
        t.prepare(&new, 2).await.unwrap();
        t.commit(&new).await.unwrap();
        assert_eq!(t.get(4).await.unwrap(), Some(vec![14]));
        assert!(
            !t.decide(&old.id, decision(true, 1))
                .await
                .unwrap()
                .committed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_retries_across_commit() {
        let t = fixture(true).await;
        let pause = (Arc::new(Barrier::new(2)), Arc::new(Barrier::new(2)));
        let scan = tokio::spawn({
            let (t, pause) = (t.clone(), pause.clone());
            async move { t.scan(1, 5, Some(&pause)).await }
        });
        pause.0.wait().await;
        let txn = transaction("scan", 1);
        t.prepare(&txn, 2).await.unwrap();
        t.commit(&txn).await.unwrap();
        pause.1.wait().await;
        let values = scan.await.unwrap().unwrap();
        assert_eq!(
            values,
            vec![(1, vec![11]), (2, vec![2]), (3, vec![3]), (4, vec![14])]
        );
    }
}
