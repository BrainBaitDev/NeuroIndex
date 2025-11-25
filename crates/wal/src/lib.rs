use crc32fast::Hasher as Crc32;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"NIWL";
const WAL_VERSION: u16 = 1;
const SNAP_MAGIC: &[u8; 4] = b"NISN";
const SNAP_VERSION: u16 = 2;
const COMPRESSED_FLAG: u16 = 0b0001;

#[derive(Debug, Clone, Copy)]
pub struct WalOptions {
    pub compress: bool,
    pub compression_level: i32,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            compress: true,
            compression_level: 3,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalRecord<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
    Tag { key: K, tag: u64 },
    Untag { key: K, tag: u64 },
    TtlSet { key: K, expires_at_ms: u64 },
    TtlRemove { key: K },
}

#[derive(Debug)]
pub struct WalWriter<K, V> {
    options: WalOptions,
    writer: BufWriter<File>,
    path: PathBuf,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> WalWriter<K, V>
where
    K: Serialize + Clone,
    V: Serialize + Clone,
{
    pub fn open<P: AsRef<Path>>(path: P, options: WalOptions) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        Ok(Self {
            options,
            writer: BufWriter::new(file),
            path: path.as_ref().to_path_buf(),
            _marker: PhantomData,
        })
    }

    pub fn append_record(&mut self, record: &WalRecord<K, V>) -> io::Result<()> {
        let encoded = bincode::serialize(record).map_err(to_io_err)?;
        let mut crc = Crc32::new();
        crc.update(&encoded);
        let mut payload = encoded.clone();
        let mut flags = 0u16;

        if self.options.compress {
            let compressed = zstd::encode_all(&encoded[..], self.options.compression_level)
                .map_err(to_io_err)?;
            if compressed.len() < encoded.len() {
                payload = compressed;
                flags |= COMPRESSED_FLAG;
            }
        }

        let header = WalHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            flags,
            original_len: encoded.len() as u32,
            stored_len: payload.len() as u32,
            crc32: crc.finalize(),
        };
        self.writer.write_all(&header.to_bytes())?;
        self.writer.write_all(&payload)?;
        Ok(())
    }

    pub fn append_put(&mut self, key: &K, value: &V) -> io::Result<()> {
        let record = WalRecord::Put {
            key: key.clone(),
            value: value.clone(),
        };
        self.append_record(&record)
    }

    pub fn append_delete(&mut self, key: &K) -> io::Result<()> {
        let record = WalRecord::Delete { key: key.clone() };
        self.append_record(&record)
    }

    pub fn append_tag(&mut self, key: &K, tag: u64) -> io::Result<()> {
        let record = WalRecord::Tag {
            key: key.clone(),
            tag,
        };
        self.append_record(&record)
    }

    pub fn append_untag(&mut self, key: &K, tag: u64) -> io::Result<()> {
        let record = WalRecord::Untag {
            key: key.clone(),
            tag,
        };
        self.append_record(&record)
    }

    pub fn append_ttl_set(&mut self, key: &K, expires_at_ms: u64) -> io::Result<()> {
        let record = WalRecord::TtlSet {
            key: key.clone(),
            expires_at_ms,
        };
        self.append_record(&record)
    }

    pub fn append_ttl_remove(&mut self, key: &K) -> io::Result<()> {
        let record = WalRecord::TtlRemove { key: key.clone() };
        self.append_record(&record)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }
}

#[derive(Debug)]
pub struct WalReader<K, V> {
    reader: BufReader<File>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> WalReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            _marker: PhantomData,
        })
    }

    pub fn next(&mut self) -> io::Result<Option<WalRecord<K, V>>> {
        let mut header_buf = [0u8; WalHeader::SIZE];
        match self.reader.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let header = WalHeader::from_bytes(&header_buf)?;
        if &header.magic != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WAL magic",
            ));
        }
        if header.version != WAL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported WAL version",
            ));
        }
        let mut payload = vec![0u8; header.stored_len as usize];
        self.reader.read_exact(&mut payload)?;
        let data = if header.flags & COMPRESSED_FLAG != 0 {
            zstd::decode_all(&payload[..]).map_err(to_io_err)?
        } else {
            payload
        };
        if data.len() != header.original_len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload length mismatch",
            ));
        }
        let mut crc = Crc32::new();
        crc.update(&data);
        if crc.finalize() != header.crc32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "crc mismatch in WAL record",
            ));
        }
        let record = bincode::deserialize::<WalRecord<K, V>>(&data).map_err(to_io_err)?;
        Ok(Some(record))
    }

    pub fn rewind(&mut self) -> io::Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotMeta {
    pub shards_pow2: u32,
    pub shard_cap_pow2: u32,
}

#[derive(Debug, Clone)]
pub struct SnapshotOptions {
    pub compression_level: i32,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            compression_level: 10,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum SnapshotRecord<K, V> {
    EntriesStart,
    Entry { key: K, value: V },
    EntriesEnd,
    TagsStart,
    Tag { key: K, tag: u64 },
    TagsEnd,
    TtlStart,
    Ttl { key: K, expires_at_ms: u64 },
    TtlEnd,
}

#[derive(Debug)]
pub struct SnapshotData<K, V> {
    pub meta: SnapshotMeta,
    pub entries: Vec<(K, V)>,
    pub tags: Vec<(K, u64)>,
    pub ttls: Vec<(K, u64)>,
}

pub fn write_snapshot<P, K, V, IE, IT, ITtl>(
    path: P,
    options: SnapshotOptions,
    meta: SnapshotMeta,
    entries: IE,
    tags: IT,
    ttls: ITtl,
) -> io::Result<()>
where
    P: AsRef<Path>,
    K: Serialize,
    V: Serialize,
    IE: IntoIterator<Item = (K, V)>,
    IT: IntoIterator<Item = (K, u64)>,
    ITtl: IntoIterator<Item = (K, u64)>,
{
    let mut file = File::create(path)?;
    file.write_all(SNAP_MAGIC)?;
    file.write_all(&SNAP_VERSION.to_le_bytes())?;
    let encoder =
        zstd::stream::write::Encoder::new(file, options.compression_level).map_err(to_io_err)?;
    let mut writer = encoder.auto_finish();
    bincode::serialize_into(&mut writer, &meta).map_err(to_io_err)?;
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::EntriesStart)
        .map_err(to_io_err)?;
    for (k, v) in entries {
        let rec = SnapshotRecord::Entry { key: k, value: v };
        bincode::serialize_into(&mut writer, &rec).map_err(to_io_err)?;
    }
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::EntriesEnd).map_err(to_io_err)?;
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::TagsStart).map_err(to_io_err)?;
    for (k, tag) in tags {
        let rec: SnapshotRecord<K, V> = SnapshotRecord::Tag { key: k, tag };
        bincode::serialize_into(&mut writer, &rec).map_err(to_io_err)?;
    }
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::TagsEnd).map_err(to_io_err)?;
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::TtlStart).map_err(to_io_err)?;
    for (k, expires_at_ms) in ttls {
        let rec: SnapshotRecord<K, V> = SnapshotRecord::Ttl { key: k, expires_at_ms };
        bincode::serialize_into(&mut writer, &rec).map_err(to_io_err)?;
    }
    bincode::serialize_into(&mut writer, &SnapshotRecord::<K, V>::TtlEnd).map_err(to_io_err)?;
    writer.flush()?;
    Ok(())
}

pub fn read_snapshot<P, K, V>(path: P) -> io::Result<Option<SnapshotData<K, V>>>
where
    P: AsRef<Path>,
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    if !path.as_ref().exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    let mut version = [0u8; 2];
    file.read_exact(&mut magic)?;
    file.read_exact(&mut version)?;
    if &magic != SNAP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid snapshot magic",
        ));
    }
    let version = u16::from_le_bytes(version);
    if version != SNAP_VERSION && version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported snapshot version",
        ));
    }
    let mut decoder = zstd::stream::read::Decoder::new(file).map_err(to_io_err)?;
    let meta: SnapshotMeta = bincode::deserialize_from(&mut decoder).map_err(to_io_err)?;
    let mut entries = Vec::new();
    let mut tags = Vec::new();
    let mut state = SnapshotState::None;
    let mut ttls = Vec::new();

    loop {
        match bincode::deserialize_from::<_, SnapshotRecord<K, V>>(&mut decoder) {
            Ok(SnapshotRecord::EntriesStart) => state = SnapshotState::Entries,
            Ok(SnapshotRecord::EntriesEnd) => state = SnapshotState::None,
            Ok(SnapshotRecord::TagsStart) => state = SnapshotState::Tags,
            Ok(SnapshotRecord::TagsEnd) => {
                if version == 1 {
                    state = SnapshotState::Finished;
                    break;
                } else {
                    state = SnapshotState::None;
                }
            }
            Ok(SnapshotRecord::TtlStart) => state = SnapshotState::Ttls,
            Ok(SnapshotRecord::TtlEnd) => {
                state = SnapshotState::Finished;
                break;
            }
            Ok(SnapshotRecord::Entry { key, value }) => {
                if state != SnapshotState::Entries {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "entry outside Entries section",
                    ));
                }
                entries.push((key, value));
            }
            Ok(SnapshotRecord::Tag { key, tag }) => {
                if state != SnapshotState::Tags {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "tag outside Tags section",
                    ));
                }
                tags.push((key, tag));
            }
            Ok(SnapshotRecord::Ttl { key, expires_at_ms }) => {
                if state != SnapshotState::Ttls {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ttl outside TTL section",
                    ));
                }
                ttls.push((key, expires_at_ms));
            }
            Err(e) => {
                if let bincode::ErrorKind::Io(ref io_err) = *e {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                return Err(to_io_err(e));
            }
        }
    }

    if state != SnapshotState::Finished && version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "snapshot ended unexpectedly",
        ));
    }

    Ok(Some(SnapshotData {
        meta,
        entries,
        tags,
        ttls,
    }))
}

#[derive(PartialEq)]
enum SnapshotState {
    None,
    Entries,
    Tags,
    Ttls,
    Finished,
}

struct WalHeader {
    magic: [u8; 4],
    version: u16,
    flags: u16,
    original_len: u32,
    stored_len: u32,
    crc32: u32,
}

impl WalHeader {
    const SIZE: usize = 4 + 2 + 2 + 4 + 4 + 4;

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..12].copy_from_slice(&self.original_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.stored_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8; Self::SIZE]) -> io::Result<Self> {
        Ok(Self {
            magic: [bytes[0], bytes[1], bytes[2], bytes[3]],
            version: u16::from_le_bytes([bytes[4], bytes[5]]),
            flags: u16::from_le_bytes([bytes[6], bytes[7]]),
            original_len: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            stored_len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            crc32: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
        })
    }
}

fn to_io_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_roundtrip() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::<u64, String>::open(&wal_path, WalOptions::default()).unwrap();
        writer.append_put(&1, &"hello".to_string()).unwrap();
        writer.append_tag(&1, 42).unwrap();
        writer.append_ttl_set(&1, 1_700_000_000_000).unwrap();
        writer.append_ttl_remove(&1).unwrap();
        writer.append_delete(&1).unwrap();
        writer.flush().unwrap();

        let mut reader = WalReader::<u64, String>::open(&wal_path).unwrap();
        match reader.next().unwrap() {
            Some(WalRecord::Put { key, value }) => {
                assert_eq!(key, 1);
                assert_eq!(value, "hello");
            }
            other => panic!("unexpected record {:?}", other),
        }
        match reader.next().unwrap() {
            Some(WalRecord::Tag { key, tag }) => {
                assert_eq!(key, 1);
                assert_eq!(tag, 42);
            }
            other => panic!("unexpected record {:?}", other),
        }
        match reader.next().unwrap() {
            Some(WalRecord::TtlSet { key, expires_at_ms }) => {
                assert_eq!(key, 1);
                assert_eq!(expires_at_ms, 1_700_000_000_000);
            }
            other => panic!("unexpected record {:?}", other),
        }
        match reader.next().unwrap() {
            Some(WalRecord::TtlRemove { key }) => assert_eq!(key, 1),
            other => panic!("unexpected record {:?}", other),
        }
        match reader.next().unwrap() {
            Some(WalRecord::Delete { key }) => assert_eq!(key, 1),
            other => panic!("unexpected record {:?}", other),
        }
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn snapshot_roundtrip() {
        let dir = tempdir().unwrap();
        let snap_path = dir.path().join("snapshot.bin");
        let entries = (0u64..16).map(|k| (k, format!("val{:#x}", k)));
        let tags = vec![(0u64, 7), (3, 9), (3, 19)];
        let ttls = vec![(0u64, 1_800_000_000_000), (3, 1_700_000_100_000)];
        write_snapshot(
            &snap_path,
            SnapshotOptions::default(),
            SnapshotMeta {
                shards_pow2: 8,
                shard_cap_pow2: 64,
            },
            entries,
            tags.clone(),
            ttls.clone(),
        )
        .unwrap();

        let data: SnapshotData<u64, String> = read_snapshot(&snap_path).unwrap().unwrap();
        assert_eq!(data.meta.shards_pow2, 8);
        assert_eq!(data.entries.len(), 16);
        assert_eq!(data.tags, tags);
        assert_eq!(data.ttls, ttls);
    }
}
