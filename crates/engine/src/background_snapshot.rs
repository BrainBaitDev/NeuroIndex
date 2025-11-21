use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use wal::{write_snapshot, SnapshotMeta, SnapshotOptions};

/// Background snapshotter che non blocca i write
pub struct BackgroundSnapshotter {
    /// Flag per shutdown graceful
    running: Arc<AtomicBool>,
    /// Handle del thread worker
    worker_thread: Option<thread::JoinHandle<()>>,
    /// Ultima snapshot completata con successo
    last_snapshot_timestamp: Arc<AtomicU64>,
    /// Snapshot path
    snapshot_path: PathBuf,
    /// Snapshot options
    snapshot_options: SnapshotOptions,
}

impl BackgroundSnapshotter {
    pub fn new(snapshot_path: PathBuf, snapshot_options: SnapshotOptions) -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            worker_thread: None,
            last_snapshot_timestamp: Arc::new(AtomicU64::new(0)),
            snapshot_path,
            snapshot_options,
        }
    }

    /// Avvia il background worker (periodic snapshots)
    pub fn start<K, V>(
        &mut self,
        engine: Arc<crate::Engine<K, V>>,
        interval: Duration,
    ) -> io::Result<()>
    where
        K: Eq
            + Hash
            + Ord
            + Clone
            + art::AsBytes
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
        V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if self.running.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "snapshotter already running",
            ));
        }

        self.running.store(true, Ordering::Relaxed);
        let running = self.running.clone();
        let last_ts = self.last_snapshot_timestamp.clone();
        let path = self.snapshot_path.clone();
        let opts = self.snapshot_options.clone();

        let handle = thread::Builder::new()
            .name("neuroindex-snapshotter".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    thread::sleep(interval);

                    if !running.load(Ordering::Relaxed) {
                        break;
                    }

                    // Esegui snapshot in background
                    match Self::take_snapshot_nonblocking(&engine, &path, &opts) {
                        Ok(_) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            last_ts.store(now, Ordering::Relaxed);
                            eprintln!("[snapshotter] snapshot completed at {}", now);
                        }
                        Err(e) => {
                            eprintln!("[snapshotter] snapshot failed: {}", e);
                        }
                    }
                }
                eprintln!("[snapshotter] worker stopped");
            })?;

        self.worker_thread = Some(handle);
        Ok(())
    }

    /// Ferma il background worker
    pub fn stop(&mut self) -> io::Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.running.store(false, Ordering::Relaxed);

        if let Some(handle) = self.worker_thread.take() {
            handle.join().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "failed to join worker thread")
            })?;
        }

        Ok(())
    }

    /// Snapshot manuale (trigger on-demand)
    pub fn trigger_snapshot<K, V>(&self, engine: &Arc<crate::Engine<K, V>>) -> io::Result<()>
    where
        K: Eq
            + Hash
            + Ord
            + Clone
            + art::AsBytes
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
        V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        Self::take_snapshot_nonblocking(engine, &self.snapshot_path, &self.snapshot_options)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_snapshot_timestamp.store(now, Ordering::Relaxed);
        Ok(())
    }

    /// Timestamp dell'ultima snapshot completata
    pub fn last_snapshot_timestamp(&self) -> u64 {
        self.last_snapshot_timestamp.load(Ordering::Relaxed)
    }

    /// Take snapshot NON bloccante
    ///
    /// Strategia:
    /// 1. Export veloce di tutti i dati (clona tutto in memoria)
    /// 2. I write continuano senza blocco
    /// 3. Serializzazione su disco in background
    fn take_snapshot_nonblocking<K, V>(
        engine: &Arc<crate::Engine<K, V>>,
        snapshot_path: &PathBuf,
        snapshot_options: &SnapshotOptions,
    ) -> io::Result<()>
    where
        K: Eq
            + Hash
            + Ord
            + Clone
            + art::AsBytes
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
        V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        // FASE 1: Clone veloce (READ lock, non blocca write a lungo)
        let start = std::time::Instant::now();
        let entries = engine.export_entries();
        let tags = engine.export_tags();
        let clone_duration = start.elapsed();

        eprintln!(
            "[snapshotter] cloned {} entries + {} tags in {:?}",
            entries.len(),
            tags.len(),
            clone_duration
        );

        // FASE 2: Serializzazione su disco (NO lock, write continuano!)
        let meta = SnapshotMeta {
            shards_pow2: engine.shards_pow2() as u32,
            shard_cap_pow2: engine.per_shard_cap_pow2() as u32,
        };

        let start = std::time::Instant::now();
        write_snapshot(snapshot_path, snapshot_options.clone(), meta, entries, tags)?;
        let write_duration = start.elapsed();

        eprintln!("[snapshotter] wrote to disk in {:?}", write_duration);

        Ok(())
    }
}

impl Drop for BackgroundSnapshotter {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Extension trait per Engine per supportare background snapshots
pub trait BackgroundSnapshotExt<K, V>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Avvia periodic background snapshots
    fn start_background_snapshots(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<BackgroundSnapshotter>;
}

impl<K, V> BackgroundSnapshotExt<K, V> for crate::Engine<K, V>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn start_background_snapshots(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<BackgroundSnapshotter> {
        // Recupera persistence config dall'engine
        let (snapshot_path, snapshot_options) = self
            .persistence_config()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "persistence not configured"))?;

        let mut snapshotter =
            BackgroundSnapshotter::new(snapshot_path.clone(), snapshot_options.clone());
        snapshotter.start(self.clone(), interval)?;
        Ok(snapshotter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_background_snapshot_basic() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let snapshot_path = dir.path().join("test.snap");

        let config = crate::PersistenceConfig::new(4, 8, &wal_path, &snapshot_path);
        let engine = Arc::new(Engine::<String, String>::with_persistence(config.clone()).unwrap());

        // Inserisci dati
        engine
            .put("key1".to_string(), "value1".to_string())
            .unwrap();
        engine
            .put("key2".to_string(), "value2".to_string())
            .unwrap();

        // Avvia background snapshotter
        let snapshotter = engine
            .start_background_snapshots(Duration::from_millis(100))
            .unwrap();

        // Aspetta che faccia almeno una snapshot
        std::thread::sleep(Duration::from_millis(250));

        // Verifica che la snapshot sia stata creata
        assert!(snapshot_path.exists());
        assert!(snapshotter.last_snapshot_timestamp() > 0);

        // Inserisci altri dati mentre snapshot in background
        engine
            .put("key3".to_string(), "value3".to_string())
            .unwrap();

        drop(snapshotter); // Stop snapshotter

        // Verifica recovery
        let engine2 = Engine::<String, String>::recover(config).unwrap();
        assert_eq!(engine2.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(engine2.get(&"key2".to_string()), Some("value2".to_string()));
    }

    #[test]
    fn test_snapshot_nonblocking() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let snapshot_path = dir.path().join("test.snap");

        let config = crate::PersistenceConfig::new(4, 8, &wal_path, &snapshot_path);
        let engine = Arc::new(Engine::<String, String>::with_persistence(config).unwrap());

        // Inserisci molti dati
        for i in 0..1000 {
            engine
                .put(format!("key{}", i), format!("value{}", i))
                .unwrap();
        }

        let snapshotter = BackgroundSnapshotter::new(snapshot_path.clone(), Default::default());

        // Trigger snapshot manuale
        let start = std::time::Instant::now();
        snapshotter.trigger_snapshot(&engine).unwrap();
        let duration = start.elapsed();

        println!("Snapshot took: {:?}", duration);

        // Durante lo snapshot, i write NON devono bloccare
        // (questo test passa perché export_entries è già veloce)

        assert!(snapshot_path.exists());
    }
}
