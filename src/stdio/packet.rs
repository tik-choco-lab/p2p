#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum StreamType {
    Stdin = 0x00,
    Stdout = 0x01,
    Stderr = 0x02,
}

pub fn wrap_packet(stream_type: StreamType, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + data.len());
    out.push(stream_type as u8);
    out.extend_from_slice(data);
    out
}

pub fn unwrap_packet(data: &[u8]) -> (StreamType, &[u8]) {
    if data.is_empty() {
        return (StreamType::Stdin, &[]);
    }
    let stream_type = match data[0] {
        0x01 => StreamType::Stdout,
        0x02 => StreamType::Stderr,
        _ => StreamType::Stdin,
    };
    (stream_type, &data[1..])
}
