use std::fmt;
use uuid::Uuid;

pub const CLASS_SUBSCRIBER: u8 = 0xf0;
pub const CLASS_CALL_CONTROL: u8 = 0xf1;
pub const CLASS_FRAME: u8 = 0xf2;
pub const CLASS_ERROR: u8 = 0xf3;
pub const CLASS_SERVICE: u8 = 0xf4;

pub const SUB_DEREGISTER: u8 = 0;
pub const SUB_REGISTER: u8 = 1;
pub const SUB_REREGISTER: u8 = 2;
pub const SUB_AFFILIATE: u8 = 8;
pub const SUB_DEAFFILIATE: u8 = 9;

pub const CALL_GROUP_TX: u8 = 2;
pub const CALL_GROUP_IDLE: u8 = 3;
pub const CALL_SETUP_REQUEST: u8 = 4;
pub const CALL_SETUP_ACCEPT: u8 = 5;
pub const CALL_SETUP_REJECT: u8 = 6;
pub const CALL_ALERT: u8 = 7;
pub const CALL_CONNECT_REQUEST: u8 = 8;
pub const CALL_CONNECT_CONFIRM: u8 = 9;
pub const CALL_RELEASE: u8 = 10;
pub const CALL_SHORT_TRANSFER: u8 = 11;
pub const CALL_SIMPLEX_GRANTED: u8 = 12;
pub const CALL_SIMPLEX_IDLE: u8 = 13;

pub const FRAME_TRAFFIC_CHANNEL: u8 = 0;
pub const FRAME_SDS_TRANSFER: u8 = 1;
pub const FRAME_SDS_REPORT: u8 = 2;

#[derive(Debug, Clone)]
pub enum BrewMessage {
    Subscriber(SubscriberMessage),
    CallControl(CallControlMessage),
    Frame(FrameMessage),
    Error(ErrorMessage),
    Service(ServiceMessage),
}

#[derive(Debug, Clone)]
pub struct SubscriberMessage {
    pub msg_type: u8,
    pub issi: u32,
    pub timestamp: u64,
    pub fraction: u32,
    pub groups: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct GroupTransmission {
    pub source: u32,
    pub destination: u32,
    pub priority: u8,
    pub access: u8,
    pub service: u16,
}

#[derive(Debug, Clone)]
pub enum CallPayload {
    GroupTransmission(GroupTransmission),
    Cause(u8),
    Empty,
    ShortTransfer { source: u32, destination: u32 },
    Raw(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct CallControlMessage {
    pub call_state: u8,
    pub identifier: Uuid,
    pub payload: CallPayload,
}

#[derive(Debug, Clone)]
pub struct FrameMessage {
    pub frame_type: u8,
    pub identifier: Uuid,
    pub length_bits: u16,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ErrorMessage {
    pub error_type: u8,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ServiceMessage {
    pub service_type: u8,
    pub json_data: String,
}

#[derive(Debug)]
pub enum ParseError {
    TooShort(usize),
    UnknownClass(u8),
    InvalidUtf8,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort(n) => write!(f, "Brew packet too short: {n} bytes"),
            Self::UnknownClass(c) => write!(f, "unknown Brew class 0x{c:02x}"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 service payload"),
        }
    }
}

impl std::error::Error for ParseError {}

fn u16le(data: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([data[o], data[o + 1]])
}

fn u32le(data: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([data[o], data[o + 1], data[o + 2], data[o + 3]])
}

fn u64le(data: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(data[o..o + 8].try_into().expect("checked length"))
}

pub fn parse(data: &[u8]) -> Result<BrewMessage, ParseError> {
    if data.len() < 2 {
        return Err(ParseError::TooShort(data.len()));
    }

    match data[0] {
        CLASS_SUBSCRIBER => parse_subscriber(data),
        CLASS_CALL_CONTROL => parse_call_control(data),
        CLASS_FRAME => parse_frame(data),
        CLASS_ERROR => Ok(BrewMessage::Error(ErrorMessage {
            error_type: data[1],
            data: data[2..].to_vec(),
        })),
        CLASS_SERVICE => parse_service(data),
        c => Err(ParseError::UnknownClass(c)),
    }
}

fn parse_subscriber(data: &[u8]) -> Result<BrewMessage, ParseError> {
    if data.len() < 18 {
        return Err(ParseError::TooShort(data.len()));
    }

    let mut groups = Vec::new();
    let mut o = 18;
    while o + 4 <= data.len() {
        groups.push(u32le(data, o));
        o += 4;
    }

    Ok(BrewMessage::Subscriber(SubscriberMessage {
        msg_type: data[1],
        issi: u32le(data, 2),
        timestamp: u64le(data, 6),
        fraction: u32le(data, 14),
        groups,
    }))
}

fn parse_call_control(data: &[u8]) -> Result<BrewMessage, ParseError> {
    if data.len() < 18 {
        return Err(ParseError::TooShort(data.len()));
    }

    let id = Uuid::from_bytes(data[2..18].try_into().expect("checked length"));
    let payload = &data[18..];
    let parsed_payload = match data[1] {
        CALL_GROUP_TX => {
            if payload.len() < 12 {
                return Err(ParseError::TooShort(data.len()));
            }
            CallPayload::GroupTransmission(GroupTransmission {
                source: u32le(payload, 0),
                destination: u32le(payload, 4),
                priority: payload[8],
                access: payload[9],
                service: u16le(payload, 10),
            })
        }
        CALL_GROUP_IDLE | CALL_SETUP_REJECT | CALL_RELEASE => {
            if payload.is_empty() {
                return Err(ParseError::TooShort(data.len()));
            }
            CallPayload::Cause(payload[0])
        }
        CALL_SETUP_ACCEPT | CALL_ALERT => CallPayload::Empty,
        CALL_SHORT_TRANSFER => {
            if payload.len() < 8 {
                return Err(ParseError::TooShort(data.len()));
            }
            CallPayload::ShortTransfer {
                source: u32le(payload, 0),
                destination: u32le(payload, 4),
            }
        }
        _ => CallPayload::Raw(payload.to_vec()),
    };

    Ok(BrewMessage::CallControl(CallControlMessage {
        call_state: data[1],
        identifier: id,
        payload: parsed_payload,
    }))
}

fn parse_frame(data: &[u8]) -> Result<BrewMessage, ParseError> {
    if data.len() < 20 {
        return Err(ParseError::TooShort(data.len()));
    }

    Ok(BrewMessage::Frame(FrameMessage {
        frame_type: data[1],
        identifier: Uuid::from_bytes(data[2..18].try_into().expect("checked length")),
        length_bits: u16le(data, 18),
        data: data[20..].to_vec(),
    }))
}

fn parse_service(data: &[u8]) -> Result<BrewMessage, ParseError> {
    let raw = &data[2..];
    let end = raw.iter().position(|b| *b == 0).unwrap_or(raw.len());
    let json_data = std::str::from_utf8(&raw[..end])
        .map_err(|_| ParseError::InvalidUtf8)?
        .to_owned();
    Ok(BrewMessage::Service(ServiceMessage {
        service_type: data[1],
        json_data,
    }))
}


pub fn build_call_cause(call_state: u8, id: &Uuid, cause: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    out.push(CLASS_CALL_CONTROL);
    out.push(call_state);
    out.extend_from_slice(id.as_bytes());
    out.push(cause);
    out
}

pub fn raw_peer_pair(payload: &CallPayload) -> Option<(u32, u32)> {
    let CallPayload::Raw(raw) = payload else { return None };
    if raw.len() < 8 { return None; }
    Some((u32le(raw, 0), u32le(raw, 4)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_group_tx() {
        let id = Uuid::new_v4();
        let mut p = vec![CLASS_CALL_CONTROL, CALL_GROUP_TX];
        p.extend_from_slice(id.as_bytes());
        p.extend_from_slice(&1001u32.to_le_bytes());
        p.extend_from_slice(&91u32.to_le_bytes());
        p.push(3);
        p.push(0);
        p.extend_from_slice(&0u16.to_le_bytes());

        let BrewMessage::CallControl(cc) = parse(&p).unwrap() else { panic!() };
        assert_eq!(cc.identifier, id);
        let CallPayload::GroupTransmission(gt) = cc.payload else { panic!() };
        assert_eq!(gt.source, 1001);
        assert_eq!(gt.destination, 91);
    }

    #[test]
    fn parses_traffic_frame() {
        let id = Uuid::new_v4();
        let mut p = vec![CLASS_FRAME, FRAME_TRAFFIC_CHANNEL];
        p.extend_from_slice(id.as_bytes());
        p.extend_from_slice(&274u16.to_le_bytes());
        p.extend_from_slice(&[0x80; 36]);

        let BrewMessage::Frame(frame) = parse(&p).unwrap() else { panic!() };
        assert_eq!(frame.identifier, id);
        assert_eq!(frame.length_bits, 274);
        assert_eq!(frame.data.len(), 36);
    }
}
