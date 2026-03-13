pub const CMD_CREATE_GROUP: u8 = 0x01;
pub const CMD_DROP_GROUP: u8 = 0x02;
pub const CMD_ADD: u8 = 0x03;
pub const CMD_ADD_RANGE: u8 = 0x04;
pub const CMD_READ: u8 = 0x05;
pub const CMD_READ_RANGE: u8 = 0x06;
pub const CMD_REMOVE: u8 = 0x07;
pub const CMD_LIST_GROUPS: u8 = 0x08;
pub const CMD_GROUP_STATS: u8 = 0x09;
pub const CMD_SET: u8 = 0x10;
pub const CMD_GET: u8 = 0x11;
pub const CMD_DEL: u8 = 0x12;
pub const CMD_KEYS: u8 = 0x13;
pub const CMD_FLUSH: u8 = 0x14;
pub const LPUSH: u8 = 0x15;
pub const LPUSH_RANGE: u8 = 0x16;
pub const LPOP: u8 = 0x17;
pub const LPOP_RANGE: u8 = 0x18;
pub const LPOP_COUNT: u8 = 0x19;
pub const LLEN: u8 = 0x20;
pub const LFLUSH: u8 = 0x21;

// ha commands

pub const HA_REGISTER: u8 = 0x22;
pub const HA_HEARTBEAT: u8 = 0x23;
pub const HA_SYNC: u8 = 0x24;

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;

#[derive(Debug)]
pub enum Command<'a> {
    CreateGroup {
        name: &'a str,
    },
    DropGroup {
        name: &'a str,
    },
    Add {
        group: &'a str,
        timestamp: u64,
        payload: &'a [u8],
    },
    AddRange {
        group: &'a str,
        entries: Vec<(u64, &'a [u8])>,
    },
    Read {
        group: &'a str,
        id: u64,
    },
    ReadRange {
        group: &'a str,
        start: u64,
        end: u64,
    },
    Remove {
        group: &'a str,
        up_to_id: u64,
    },
    ListGroups,
    GroupStats {
        group: &'a str,
    },
    SetKey {
        db: &'a str,
        key: &'a str,
        val: &'a [u8],
        ttl_secs: u64,
    },
    GetKey {
        db: &'a str,
        key: &'a str,
    },
    DelKey {
        db: &'a str,
        key: &'a str,
    },
    Keys {
        db: &'a str,
    },
    Flush {
        db: &'a str,
    },
    LPush {
        key: &'a str,
        val: &'a [u8],
    },
    LPushRange {
        key: &'a str,
        vals: Vec<&'a [u8]>,
    },
    LPop {
        key: &'a str,
    },
    LPopRange {
        key: &'a str,
        start: u32,
        end: u32,
    },
    LPopCount {
        key: &'a str,
        count: u32,
    },
    LLen {
        key: &'a str,
    },
    LFlush {
        key: &'a str,
    },
    StartPipeline {},
    EndPipeline {
        id: u64,
    },
    //     HA_REGISTER
    // HA_HEARTBEAT
    // HA_SYNC
    HaRegisterFollower {
        addr: &'a str, // follower send register message with its address and master will add it to followers list
    },
    HaHeartBeat {}, // master send heartbeat message and expect an ACK
    HaSync {
        entries: Vec<&'a [u8]>,
    }, // follower send a sync message and receive AOF file
}

#[derive(Debug)]
pub struct ParseError(pub &'static str);

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    #[inline(always)]
    fn read_u8(&mut self) -> Result<u8, ParseError> {
        if self.remaining() < 1 {
            return Err(ParseError("unexpected EOF reading u8"));
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    #[inline(always)]
    fn read_u16_le(&mut self) -> Result<u16, ParseError> {
        if self.remaining() < 2 {
            return Err(ParseError("unexpected EOF reading u16"));
        }
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    #[inline(always)]
    fn read_u32_le(&mut self) -> Result<u32, ParseError> {
        if self.remaining() < 4 {
            return Err(ParseError("unexpected EOF reading u32"));
        }
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    #[inline(always)]
    fn read_u64_le(&mut self) -> Result<u64, ParseError> {
        if self.remaining() < 8 {
            return Err(ParseError("unexpected EOF reading u64"));
        }
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    #[inline(always)]
    fn read_str(&mut self) -> Result<&'a str, ParseError> {
        let len = self.read_u16_le()? as usize;
        if self.remaining() < len {
            return Err(ParseError("unexpected EOF reading string"));
        }
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])
            .map_err(|_| ParseError("invalid UTF-8"))?;
        self.pos += len;
        Ok(s)
    }

    #[inline(always)]
    fn read_bytes(&mut self) -> Result<&'a [u8], ParseError> {
        let len = self.read_u32_le()? as usize;
        if self.remaining() < len {
            return Err(ParseError("unexpected EOF reading bytes"));
        }
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }
}

pub fn parse_command(frame: &[u8]) -> Result<Command<'_>, ParseError> {
    if frame.is_empty() {
        return Err(ParseError("empty frame"));
    }

    let mut cur = Cursor::new(frame);
    let tag = cur.read_u8()?;

    match tag {
        CMD_CREATE_GROUP => Ok(Command::CreateGroup {
            name: cur.read_str()?,
        }),
        CMD_DROP_GROUP => Ok(Command::DropGroup {
            name: cur.read_str()?,
        }),
        CMD_ADD => {
            let group = cur.read_str()?;
            let timestamp = cur.read_u64_le()?;
            let payload = cur.read_bytes()?;
            Ok(Command::Add {
                group,
                timestamp,
                payload,
            })
        }
        CMD_ADD_RANGE => {
            let group = cur.read_str()?;
            let count = cur.read_u32_le()? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let ts = cur.read_u64_le()?;
                let payload = cur.read_bytes()?;
                entries.push((ts, payload));
            }
            Ok(Command::AddRange { group, entries })
        }
        CMD_READ => {
            let group = cur.read_str()?;
            let id = cur.read_u64_le()?;
            Ok(Command::Read { group, id })
        }
        CMD_READ_RANGE => {
            let group = cur.read_str()?;
            let start = cur.read_u64_le()?;
            let end = cur.read_u64_le()?;
            Ok(Command::ReadRange { group, start, end })
        }
        CMD_REMOVE => {
            let group = cur.read_str()?;
            let up_to_id = cur.read_u64_le()?;
            Ok(Command::Remove { group, up_to_id })
        }
        CMD_LIST_GROUPS => Ok(Command::ListGroups),
        CMD_GROUP_STATS => Ok(Command::GroupStats {
            group: cur.read_str()?,
        }),
        CMD_SET => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            let ttl_secs = cur.read_u64_le()?;
            let val = cur.read_bytes()?;
            Ok(Command::SetKey {
                db,
                key,
                val,
                ttl_secs,
            })
        }
        CMD_GET => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            Ok(Command::GetKey { db, key })
        }
        CMD_DEL => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            Ok(Command::DelKey { db, key })
        }
        CMD_KEYS => Ok(Command::Keys {
            db: cur.read_str()?,
        }),
        CMD_FLUSH => Ok(Command::Flush {
            db: cur.read_str()?,
        }),
        LPUSH => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            let val = cur.read_bytes()?;
            Ok(Command::LPush { key, val })
        }
        LPUSH_RANGE => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            let count = cur.read_u32_le()? as usize;
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                vals.push(cur.read_bytes()?);
            }
            Ok(Command::LPushRange { key, vals })
        }
        LPOP => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            Ok(Command::LPop { key })
        }
        LPOP_RANGE => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            let start = cur.read_u32_le()?;
            let end = cur.read_u32_le()?;
            Ok(Command::LPopRange { key, start, end })
        }
        LPOP_COUNT => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            let count = cur.read_u32_le()?;
            Ok(Command::LPopCount { key, count })
        }
        LLEN => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            Ok(Command::LLen { key })
        }
        LFLUSH => {
            let db = cur.read_str()?;
            let key = cur.read_str()?;
            Ok(Command::LFlush { key })
        }
        HA_REGISTER => {
            let addr = cur.read_str()?;
            Ok(Command::HaRegisterFollower { addr: addr })
        }
        HA_HEARTBEAT => Ok(Command::HaHeartBeat {}),
        HA_SYNC => {
            let count = cur.read_u32_le()? as usize;
            let mut vals: Vec<&[u8]> = Vec::with_capacity(count);
            for _ in 0..count {
                vals.push(cur.read_bytes()?);
            }
            Ok(Command::HaSync { entries: vals })
        }

        _ => Err(ParseError("unknown command tag")),
    }
}

pub struct ResponseBuilder {
    buf: Vec<u8>,
}

impl ResponseBuilder {
    pub fn new() -> Self {
        ResponseBuilder {
            buf: Vec::with_capacity(4096),
        }
    }

    pub fn ok_empty(&mut self) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.finalize()
    }

    pub fn ok_u64(&mut self, val: u64) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf.extend_from_slice(&val.to_le_bytes());
        self.finalize()
    }

    pub fn ok_u64_pair(&mut self, a: u64, b: u64) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf.extend_from_slice(&a.to_le_bytes());
        self.buf.extend_from_slice(&b.to_le_bytes());
        self.finalize()
    }

    pub fn ok_entry(&mut self, id: u64, ts: u64, payload: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf.extend_from_slice(&id.to_le_bytes());
        self.buf.extend_from_slice(&ts.to_le_bytes());
        self.buf
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(payload);
        self.finalize()
    }

    pub fn ok_entries(&mut self, entries: &[(u64, u64, Vec<u8>)]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf
            .extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (id, ts, payload) in entries {
            self.buf.extend_from_slice(&id.to_le_bytes());
            self.buf.extend_from_slice(&ts.to_le_bytes());
            self.buf
                .extend_from_slice(&(payload.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(payload);
        }
        self.finalize()
    }

    pub fn ok_group_list(&mut self, groups: Vec<String>) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf
            .extend_from_slice(&(groups.len() as u32).to_le_bytes());
        for name in groups {
            self.buf
                .extend_from_slice(&(name.len() as u16).to_le_bytes());
            self.buf.extend_from_slice(name.as_bytes());
        }
        self.finalize()
    }

    pub fn ok_stats(&mut self, entries: u64, segments: usize, next_id: u64) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf.extend_from_slice(&entries.to_le_bytes());
        self.buf.extend_from_slice(&(segments as u32).to_le_bytes());
        self.buf.extend_from_slice(&next_id.to_le_bytes());
        self.finalize()
    }

    pub fn ok_bytes(&mut self, val: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf
            .extend_from_slice(&(val.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(val);
        self.finalize()
    }

    pub fn ok_string_list(&mut self, keys: &[String]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf
            .extend_from_slice(&(keys.len() as u32).to_le_bytes());
        for key in keys {
            self.buf
                .extend_from_slice(&(key.len() as u16).to_le_bytes());
            self.buf.extend_from_slice(key.as_bytes());
        }
        self.finalize()
    }

    pub fn ok_bytes_list(&mut self, items: &[Vec<u8>]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_OK);
        self.buf
            .extend_from_slice(&(items.len() as u32).to_le_bytes());
        for item in items {
            self.buf
                .extend_from_slice(&(item.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(item);
        }
        self.finalize()
    }

    pub fn err(&mut self, msg: &str) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(&[0u8; 4]);
        self.buf.push(STATUS_ERR);
        self.buf
            .extend_from_slice(&(msg.len() as u16).to_le_bytes());
        self.buf.extend_from_slice(msg.as_bytes());
        self.finalize()
    }

    fn finalize(&mut self) -> &[u8] {
        let body_len = (self.buf.len() - 4) as u32;
        let len_bytes = body_len.to_le_bytes();
        self.buf[0] = len_bytes[0];
        self.buf[1] = len_bytes[1];
        self.buf[2] = len_bytes[2];
        self.buf[3] = len_bytes[3];
        &self.buf
    }
}
