use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct StreamClient {
    stream: TcpStream,
    #[allow(dead_code)]
    read_buf: BytesMut,
}

impl StreamClient {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(StreamClient {
            stream,
            read_buf: BytesMut::with_capacity(8192),
        })
    }

    pub fn is_alive(&self) -> bool {
        match self.stream.try_read(&mut [0u8; 0]) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
            Ok(0) => false,
            Err(_) => false,
            Ok(_) => true,
        }
    }

    async fn send_recv(&mut self, frame: &[u8]) -> anyhow::Result<Vec<u8>> {
        let start = std::time::Instant::now();
        tracing::info!("sending frame of length {}", frame.len());

        let body_len = frame.len() as u32;
        self.stream.write_all(&body_len.to_le_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        let mut resp = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp).await?;
        let duration = start.elapsed();
        tracing::info!("received frame of length {} in {:?}", resp_len, duration);

        Ok(resp)
    }

    fn build_cmd_with_group(tag: u8, group: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 2 + group.len());
        buf.push(tag);
        buf.extend_from_slice(&(group.len() as u16).to_le_bytes());
        buf.extend_from_slice(group.as_bytes());
        buf
    }

    pub async fn create_group(&mut self, name: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_group(0x01, name);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn drop_group(&mut self, name: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_group(0x02, name);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn add(
        &mut self,
        group: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> anyhow::Result<Result<u64, String>> {
        let mut frame = Vec::with_capacity(1 + 2 + group.len() + 8 + 4 + payload.len());
        frame.push(0x03);
        frame.extend_from_slice(&(group.len() as u16).to_le_bytes());
        frame.extend_from_slice(group.as_bytes());
        frame.extend_from_slice(&timestamp.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 9 {
            Ok(Ok(u64::from_le_bytes(resp[1..9].try_into().unwrap())))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])],
    ) -> anyhow::Result<Result<(u64, u64), String>> {
        let mut frame = Vec::new();
        frame.push(0x04);
        frame.extend_from_slice(&(group.len() as u16).to_le_bytes());
        frame.extend_from_slice(group.as_bytes());
        frame.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for &(ts, payload) in entries {
            frame.extend_from_slice(&ts.to_le_bytes());
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(payload);
        }

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 17 {
            let first = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let last = u64::from_le_bytes(resp[9..17].try_into().unwrap());
            Ok(Ok((first, last)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn read(
        &mut self,
        group: &str,
        id: u64,
    ) -> anyhow::Result<Result<(u64, u64, Vec<u8>), String>> {
        let mut frame = Self::build_cmd_with_group(0x05, group);
        frame.extend_from_slice(&id.to_le_bytes());

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 21 {
            let entry_id = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let ts = u64::from_le_bytes(resp[9..17].try_into().unwrap());
            let plen = u32::from_le_bytes(resp[17..21].try_into().unwrap()) as usize;
            let payload = resp[21..21 + plen].to_vec();
            Ok(Ok((entry_id, ts, payload)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> anyhow::Result<Result<Vec<(u64, u64, Vec<u8>)>, String>> {
        let mut frame = Self::build_cmd_with_group(0x06, group);
        frame.extend_from_slice(&start.to_le_bytes());
        frame.extend_from_slice(&end.to_le_bytes());

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 5 {
            let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
            let mut entries = Vec::with_capacity(count);
            let mut pos = 5;
            for _ in 0..count {
                let id = u64::from_le_bytes(resp[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let ts = u64::from_le_bytes(resp[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let plen = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let payload = resp[pos..pos + plen].to_vec();
                pos += plen;
                entries.push((id, ts, payload));
            }
            Ok(Ok(entries))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn remove(
        &mut self,
        group: &str,
        up_to_id: u64,
    ) -> anyhow::Result<Result<(), String>> {
        let mut frame = Self::build_cmd_with_group(0x07, group);
        frame.extend_from_slice(&up_to_id.to_le_bytes());
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn list_groups(&mut self) -> anyhow::Result<Result<Vec<String>, String>> {
        let frame = vec![0x08];
        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 5 {
            let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
            let mut groups = Vec::with_capacity(count);
            let mut pos = 5;
            for _ in 0..count {
                let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                let name = String::from_utf8_lossy(&resp[pos..pos + name_len]).to_string();
                pos += name_len;
                groups.push(name);
            }
            Ok(Ok(groups))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn group_stats(
        &mut self,
        group: &str,
    ) -> anyhow::Result<Result<(u64, u32, u64), String>> {
        let frame = Self::build_cmd_with_group(0x09, group);
        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 21 {
            let entries = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let segments = u32::from_le_bytes(resp[9..13].try_into().unwrap());
            let next_id = u64::from_le_bytes(resp[13..21].try_into().unwrap());
            Ok(Ok((entries, segments, next_id)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }
    fn build_cmd_with_db_key(tag: u8, db: &str, key: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 2 + db.len() + 2 + key.len());
        buf.push(tag);
        buf.extend_from_slice(&(db.len() as u16).to_le_bytes());
        buf.extend_from_slice(db.as_bytes());
        buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf
    }

    pub async fn kv_set(
        &mut self,
        db: &str,
        key: &str,
        val: &[u8],
        ttl_secs: u64,
    ) -> anyhow::Result<Result<(), String>> {
        let mut frame = Vec::with_capacity(1 + 2 + db.len() + 2 + key.len() + 8 + 4 + val.len());
        frame.push(0x10);
        frame.extend_from_slice(&(db.len() as u16).to_le_bytes());
        frame.extend_from_slice(db.as_bytes());
        frame.extend_from_slice(&(key.len() as u16).to_le_bytes());
        frame.extend_from_slice(key.as_bytes());
        frame.extend_from_slice(&ttl_secs.to_le_bytes());
        frame.extend_from_slice(&(val.len() as u32).to_le_bytes());
        frame.extend_from_slice(val);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn kv_get(
        &mut self,
        db: &str,
        key: &str,
    ) -> anyhow::Result<Result<Option<Vec<u8>>, String>> {
        let frame = Self::build_cmd_with_db_key(0x11, db, key);
        let resp = self.send_recv(&frame).await?;
        if resp[0] != 0x00 {
            return Ok(Err(Self::extract_err(&resp)));
        }
        if resp.len() < 5 {
            return Ok(Ok(None));
        }
        let val_len = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
        if val_len == 0 {
            return Ok(Ok(None));
        }
        Ok(Ok(Some(resp[5..5 + val_len].to_vec())))
    }

    pub async fn kv_del(&mut self, db: &str, key: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_db_key(0x12, db, key);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn kv_keys(&mut self, db: &str) -> anyhow::Result<Result<Vec<String>, String>> {
        let mut frame = Vec::with_capacity(1 + 2 + db.len());
        frame.push(0x13);
        frame.extend_from_slice(&(db.len() as u16).to_le_bytes());
        frame.extend_from_slice(db.as_bytes());
        let resp = self.send_recv(&frame).await?;
        if resp[0] != 0x00 {
            return Ok(Err(Self::extract_err(&resp)));
        }
        let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
        let mut keys = Vec::with_capacity(count);
        let mut pos = 5;
        for _ in 0..count {
            let key_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let key = String::from_utf8_lossy(&resp[pos..pos + key_len]).to_string();
            pos += key_len;
            keys.push(key);
        }
        Ok(Ok(keys))
    }

    pub async fn kv_flush(&mut self, db: &str) -> anyhow::Result<Result<(), String>> {
        let mut frame = Vec::with_capacity(1 + 2 + db.len());
        frame.push(0x14);
        frame.extend_from_slice(&(db.len() as u16).to_le_bytes());
        frame.extend_from_slice(db.as_bytes());
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    fn check_status(resp: &[u8]) -> Result<(), String> {
        if resp.is_empty() {
            return Err("empty response".into());
        }
        if resp[0] == 0x00 {
            Ok(())
        } else {
            Err(Self::extract_err(resp))
        }
    }
    fn build_cmd_with_key(tag: u8, key: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 2 + key.len());
        buf.push(tag);
        buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf
    }

    pub async fn l_push(&mut self, key: &str, val: &[u8]) -> anyhow::Result<Result<(), String>> {
        let mut frame = Vec::with_capacity(1 + 2 + key.len() + 4 + val.len());
        frame.push(0x20);
        frame.extend_from_slice(&(key.len() as u16).to_le_bytes());
        frame.extend_from_slice(key.as_bytes());
        frame.extend_from_slice(&(val.len() as u32).to_le_bytes());
        frame.extend_from_slice(val);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn l_push_range(
        &mut self,
        key: &str,
        vals: &[&[u8]],
    ) -> anyhow::Result<Result<(), String>> {
        let mut frame = Vec::new();
        frame.push(0x21);
        frame.extend_from_slice(&(key.len() as u16).to_le_bytes());
        frame.extend_from_slice(key.as_bytes());
        frame.extend_from_slice(&(vals.len() as u32).to_le_bytes());
        for val in vals {
            frame.extend_from_slice(&(val.len() as u32).to_le_bytes());
            frame.extend_from_slice(val);
        }
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn l_pop(&mut self, key: &str) -> anyhow::Result<Result<Option<Vec<u8>>, String>> {
        let frame = Self::build_cmd_with_key(0x22, key);
        let resp = self.send_recv(&frame).await?;
        if resp[0] != 0x00 {
            return Ok(Err(Self::extract_err(&resp)));
        }
        if resp.len() < 5 {
            return Ok(Ok(None));
        }
        let val_len = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
        if val_len == 0 {
            return Ok(Ok(None));
        }
        Ok(Ok(Some(resp[5..5 + val_len].to_vec())))
    }

    pub async fn l_pop_range(
        &mut self,
        key: &str,
        start: u32,
        end: u32,
    ) -> anyhow::Result<Result<Vec<Vec<u8>>, String>> {
        let mut frame = Self::build_cmd_with_key(0x23, key);
        frame.extend_from_slice(&start.to_le_bytes());
        frame.extend_from_slice(&end.to_le_bytes());
        let resp = self.send_recv(&frame).await?;
        if resp[0] != 0x00 {
            return Ok(Err(Self::extract_err(&resp)));
        }
        let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
        let mut vals = Vec::with_capacity(count);
        let mut pos = 5;
        for _ in 0..count {
            let vlen = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            vals.push(resp[pos..pos + vlen].to_vec());
            pos += vlen;
        }
        Ok(Ok(vals))
    }

    pub async fn l_pop_count(
        &mut self,
        key: &str,
        count: u32,
    ) -> anyhow::Result<Result<Vec<Vec<u8>>, String>> {
        let mut frame = Self::build_cmd_with_key(0x24, key);
        frame.extend_from_slice(&count.to_le_bytes());
        let resp = self.send_recv(&frame).await?;
        if resp[0] != 0x00 {
            return Ok(Err(Self::extract_err(&resp)));
        }
        let cnt = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
        let mut vals = Vec::with_capacity(cnt);
        let mut pos = 5;
        for _ in 0..cnt {
            let vlen = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            vals.push(resp[pos..pos + vlen].to_vec());
            pos += vlen;
        }
        Ok(Ok(vals))
    }

    pub async fn l_len(&mut self, key: &str) -> anyhow::Result<Result<u64, String>> {
        let frame = Self::build_cmd_with_key(0x25, key);
        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 9 {
            Ok(Ok(u64::from_le_bytes(resp[1..9].try_into().unwrap())))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn l_flush(&mut self, key: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_key(0x26, key);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    fn extract_err(resp: &[u8]) -> String {
        if resp.len() < 4 {
            return "unknown error".into();
        }
        let msg_len = u16::from_le_bytes([resp[1], resp[2]]) as usize;
        if resp.len() >= 3 + msg_len {
            String::from_utf8_lossy(&resp[3..3 + msg_len]).to_string()
        } else {
            "malformed error response".into()
        }
    }
}
