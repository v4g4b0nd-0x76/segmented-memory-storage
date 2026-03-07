use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Length-prefixed framing: [u32 LE frame_len][frame_len bytes of payload]

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

pub struct LengthPrefixCodec;

impl Decoder for LengthPrefixCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None); // not enough for length header
        }

        // peek at length without consuming
        let len_bytes: [u8; 4] = [src[0], src[1], src[2], src[3]];
        let frame_len = u32::from_le_bytes(len_bytes) as usize;

        if frame_len > MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame too large: {} bytes", frame_len),
            ));
        }

        let total = 4 + frame_len;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }

        src.advance(4);

        let frame = src.split_to(frame_len);
        Ok(Some(frame))
    }
}

impl Encoder<Vec<u8>> for LengthPrefixCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len());
        dst.put_slice(&item);
        Ok(())
    }
}

impl Encoder<&[u8]> for LengthPrefixCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: &[u8], dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len());
        dst.put_slice(item);
        Ok(())
    }
}
