//! BasisVR protocol 57 screen share plugin payloads (2e6b44de394d).
use crate::{
    io::{NetReader, NetWriter, WriteResult},
    messages::BasisSerialize,
};
use anyhow::{ensure, Result};

pub const CONTROL_ID: u16 = 65;
pub const MEDIA_ID: u16 = 66;
pub const CONTROL_MESSAGE: &str = "basis.screenshare.control";
pub const MEDIA_MESSAGE: &str = "basis.screenshare.media";
pub const MAX_CHUNK_PAYLOAD: usize = 1100;
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CHUNKS_PER_FRAME: usize = MAX_FRAME_BYTES.div_ceil(MAX_CHUNK_PAYLOAD);
pub const START: u8 = 0;
pub const STOP: u8 = 1;
pub const POSE: u8 = 2;
pub const WATCH: u8 = 3;
pub const KEYFRAME: u8 = 4;
pub const INFO: u8 = 5;
pub const LOCK: u8 = 6;
pub const MOVE: u8 = 7;
pub const ACCEPTED: u8 = 10;
pub const REFUSED: u8 = 11;
pub const ANNOUNCE: u8 = 12;
pub const ENDED: u8 = 13;
pub const VIEWERS: u8 = 16;
pub const POLICY: u8 = 17;
pub const REFUSED_NO_PERMISSION: u8 = 1;
pub const REFUSED_TOO_MANY: u8 = 2;
pub const REFUSED_INVALID: u8 = 3;
pub const REFUSED_LOCKED: u8 = 4;
pub const KIND_VIDEO: u8 = 0;
pub const KIND_AUDIO: u8 = 1;
pub const FLAG_KEY: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub width: u16,
    pub height: u16,
    pub fps: u8,
    pub has_audio: bool,
    pub title: String,
}
impl StreamInfo {
    pub fn read(r: &mut NetReader<'_>) -> Result<Self> {
        let mut info = Self {
            width: r.get_u16()?,
            height: r.get_u16()?,
            fps: r.get_u8()?,
            has_audio: r.get_bool()?,
            title: r.get_string()?,
        };
        info.title = bounded_title(&info.title).to_owned();
        ensure!(
            (16..=4096).contains(&info.width)
                && (16..=4096).contains(&info.height)
                && (1..=120).contains(&info.fps),
            "invalid screen stream info"
        );
        Ok(info)
    }
}
impl BasisSerialize for StreamInfo {
    fn serialize(&self, w: &mut NetWriter) -> WriteResult<()> {
        w.put_u16(self.width);
        w.put_u16(self.height);
        w.put_u8(self.fps);
        w.put_bool(self.has_audio);
        w.put_string(bounded_title(&self.title))
    }
}
// C# limits the title to 64 UTF-16 code units. Keep UTF-8 boundaries intact.
fn bounded_title(title: &str) -> &str {
    let mut units = 0;
    for (index, c) in title.char_indices() {
        units += c.len_utf16();
        if units > 64 {
            return &title[..index];
        }
    }
    title
}
#[derive(Debug, Clone, PartialEq)]
pub struct ScreenPose {
    pub position: [f32; 3],
    pub rotation: [f32; 4],
    pub scale: f32,
}
impl ScreenPose {
    pub fn read(r: &mut NetReader<'_>) -> Result<Self> {
        let p = Self {
            position: [r.get_f32()?, r.get_f32()?, r.get_f32()?],
            rotation: [r.get_f32()?, r.get_f32()?, r.get_f32()?, r.get_f32()?],
            scale: r.get_f32()?,
        };
        let len: f32 = p.rotation.iter().map(|v| v * v).sum();
        ensure!(
            p.position
                .iter()
                .all(|v| v.is_finite() && v.abs() <= 100000.)
                && p.rotation.iter().all(|v| v.is_finite())
                && len > 0.5
                && len < 1.5
                && p.scale.is_finite()
                && (0.05..=20.).contains(&p.scale),
            "invalid screen pose"
        );
        Ok(p)
    }
}
impl BasisSerialize for ScreenPose {
    fn serialize(&self, w: &mut NetWriter) -> WriteResult<()> {
        for v in self
            .position
            .iter()
            .chain(self.rotation.iter())
            .chain(std::iter::once(&self.scale))
        {
            w.put_f32(*v);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHeader {
    pub sequence: u32,
    pub kind: u8,
    pub flags: u8,
    pub index: u16,
    pub count: u16,
    pub pts_us: i64,
    pub total_length: i32,
}
impl ChunkHeader {
    pub fn read(r: &mut NetReader<'_>) -> Result<Self> {
        let mut h = Self {
            sequence: r.get_u32()?,
            kind: r.get_u8()?,
            flags: r.get_u8()?,
            index: r.get_u16()?,
            count: r.get_u16()?,
            pts_us: 0,
            total_length: 0,
        };
        ensure!(
            h.kind <= KIND_AUDIO
                && h.count > 0
                && usize::from(h.count) <= MAX_CHUNKS_PER_FRAME
                && h.index < h.count,
            "invalid screen chunk header"
        );
        if h.index == 0 {
            h.pts_us = r.get_i64()?;
            h.total_length = r.get_i32()?;
            ensure!(
                h.total_length > 0
                    && h.total_length as usize <= MAX_FRAME_BYTES
                    && (h.total_length as usize).div_ceil(MAX_CHUNK_PAYLOAD)
                        == usize::from(h.count),
                "invalid screen frame length"
            );
        }
        ensure!(
            r.remaining() > 0 && r.remaining() <= MAX_CHUNK_PAYLOAD,
            "invalid screen chunk payload"
        );
        Ok(h)
    }
    pub fn is_key(&self) -> bool {
        self.flags & FLAG_KEY != 0
    }
}
impl BasisSerialize for ChunkHeader {
    fn serialize(&self, w: &mut NetWriter) -> WriteResult<()> {
        w.put_u32(self.sequence);
        w.put_u8(self.kind);
        w.put_u8(self.flags);
        w.put_u16(self.index);
        w.put_u16(self.count);
        if self.index == 0 {
            w.put_i64(self.pts_us);
            w.put_i32(self.total_length);
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunk_wire_layout_and_validation() {
        let h = ChunkHeader {
            sequence: 0x12345678,
            kind: 0,
            flags: 1,
            index: 0,
            count: 1,
            pts_us: 42,
            total_length: 3,
        };
        let mut w = NetWriter::new();
        h.serialize(&mut w).unwrap();
        w.put_bytes(&[1, 2, 3]);
        assert_eq!(
            &w.as_slice()[..10],
            &[0x78, 0x56, 0x34, 0x12, 0, 1, 0, 0, 1, 0]
        );
        assert_eq!(
            ChunkHeader::read(&mut NetReader::new(w.as_slice())).unwrap(),
            h
        );
        assert!(ChunkHeader::read(&mut NetReader::new(&w.as_slice()[..22])).is_err());
        let mut invalid = w.into_vec();
        invalid[8] = 2;
        assert!(ChunkHeader::read(&mut NetReader::new(&invalid)).is_err());
    }
    #[test]
    fn pose_rejects_nonfinite_and_invalid_quaternion() {
        let p = ScreenPose {
            position: [0.; 3],
            rotation: [0., 0., 0., 1.],
            scale: 1.,
        };
        let mut w = NetWriter::new();
        p.serialize(&mut w).unwrap();
        assert_eq!(
            ScreenPose::read(&mut NetReader::new(w.as_slice())).unwrap(),
            p
        );
        let mut bad = w.into_vec();
        bad[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(ScreenPose::read(&mut NetReader::new(&bad)).is_err());
    }
}
