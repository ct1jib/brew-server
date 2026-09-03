use crate::config::AprsConfig;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc, RwLock},
};
use tracing::{debug, error, info, warn};

const LIP_PROTOCOL_ID: u8 = 0x0a;

#[derive(Debug, Clone)]
pub struct AprsUser {
    pub callsign: String,
    pub fname: String,
}

#[derive(Debug, Deserialize)]
struct UsersFile {
    users: Vec<UserRecord>,
}

#[derive(Debug, Deserialize)]
struct UserRecord {
    radio_id: u32,
    callsign: String,
    fname: String,
}

#[derive(Debug, Clone)]
pub struct LipPosition {
    pub latitude: f64,
    pub longitude: f64,
    pub position_error_raw: u8,
    pub velocity_raw: u8,
    pub direction_raw: u8,
    pub direction_deg: f64,
    pub reason_or_user_data: u8,
}

#[derive(Debug, Clone)]
struct AprsPacket {
    line: String,
}

#[derive(Clone)]
pub struct AprsGateway {
    tx: mpsc::UnboundedSender<AprsPacket>,
    users: Arc<RwLock<HashMap<u32, AprsUser>>>,
    config: AprsConfig,
}

impl AprsGateway {
    pub fn new(config: AprsConfig) -> Self {
        let users = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let gateway = Self {
            tx,
            users: users.clone(),
            config: config.clone(),
        };

        {
            let users = users.clone();
            let cfg = config.clone();
            tokio::spawn(async move {
                reload_users_loop(cfg, users).await;
            });
        }

        tokio::spawn(async move {
            aprsis_worker(config, rx).await;
        });

        gateway
    }

    pub async fn handle_lip(&self, source_issi: u32, data: &[u8], length_bits: u16) {
        let user = {
            let users = self.users.read().await;
            users.get(&source_issi).cloned()
        };

        let Some(user) = user else {
            warn!(
                source_issi,
                payload_bits = length_bits,
                payload_hex = %hex(data),
                "APRS: ISSI not found in users.json; LIP ignored"
            );
            return;
        };

        let pos = match decode_short_lip(data, length_bits) {
            Ok(pos) => pos,
            Err(e) => {
                warn!(
                    source_issi,
                    callsign = %user.callsign,
                    first_name = %user.fname,
                    payload_bits = length_bits,
                    payload_hex = %hex(data),
                    error = %e,
                    "APRS: unable to decode LIP"
                );
                return;
            }
        };

        let packet = build_aprs_position(
            &user.callsign,
            &pos,
            self.config.symbol_table,
            self.config.symbol,
        );

        info!(
            source_issi,
            callsign = %user.callsign,
            first_name = %user.fname,
            latitude = pos.latitude,
            longitude = pos.longitude,
            direction = pos.direction_deg,
            velocity_raw = pos.velocity_raw,
            "APRS: LIP decoded"
        );

        if let Err(e) = self.tx.send(AprsPacket { line: packet }) {
            error!(error = %e, "APRS: queue unavailable");
        }
    }
}

async fn reload_users_loop(config: AprsConfig, users: Arc<RwLock<HashMap<u32, AprsUser>>>) {
    loop {
        match load_users(&config.users_file) {
            Ok(map) => {
                let count = map.len();
                *users.write().await = map;
                info!(count, file = %config.users_file.display(), "APRS: users.json loaded");
            }
            Err(e) => {
                warn!(error = %e, file = %config.users_file.display(), "APRS: unable to load users.json");
            }
        }

        tokio::time::sleep(Duration::from_secs(config.users_reload_seconds.max(5))).await;
    }
}

fn load_users(path: &std::path::Path) -> Result<HashMap<u32, AprsUser>, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let decoded: UsersFile =
        serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {e}"))?;

    let mut out = HashMap::with_capacity(decoded.users.len());
    for u in decoded.users {
        let callsign = u.callsign.trim().to_ascii_uppercase();
        if callsign.is_empty() {
            continue;
        }
        out.insert(
            u.radio_id,
            AprsUser {
                callsign,
                fname: u.fname.trim().to_owned(),
            },
        );
    }
    Ok(out)
}

async fn aprsis_worker(config: AprsConfig, mut rx: mpsc::UnboundedReceiver<AprsPacket>) {
    loop {
        let address = format!("{}:{}", config.server, config.port);
        info!(server = %address, "APRS: connecting to APRS-IS");

        match TcpStream::connect(&address).await {
            Ok(mut stream) => {
                let login = format!(
                    "user {} pass {} vers brew-server-aprs 0.1\r\n",
                    config.login_callsign.trim().to_ascii_uppercase(),
                    config.passcode
                );

                if let Err(e) = stream.write_all(login.as_bytes()).await {
                    warn!(error = %e, "APRS: login write failed");
                } else {
                    info!(server = %address, "APRS: connected to APRS-IS");

                    while let Some(packet) = rx.recv().await {
                        let line = format!("{}\r\n", packet.line);
                        debug!(packet = %packet.line, "APRS: sending packet");

                        if let Err(e) = stream.write_all(line.as_bytes()).await {
                            warn!(error = %e, "APRS: connection lost while sending");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(server = %address, error = %e, "APRS: connection failed");
            }
        }

        tokio::time::sleep(Duration::from_secs(config.reconnect_seconds.max(5))).await;
    }
}

pub fn decode_short_lip(data: &[u8], length_bits: u16) -> Result<LipPosition, String> {
    // LIP over SDS uses protocol identifier 10 (0x0A), followed by the LIP PDU.
    // SHORT LOCATION REPORT = 76 bits, so the complete SDS user payload is 84 bits.
    if length_bits < 84 {
        return Err(format!("short payload: {length_bits} bits (need at least 84)"));
    }
    if data.is_empty() || data[0] != LIP_PROTOCOL_ID {
        return Err(format!(
            "unexpected protocol id: expected 0x0A, got {}",
            data.first()
                .map(|v| format!("0x{v:02X}"))
                .unwrap_or_else(|| "<none>".into())
        ));
    }

    let mut r = BitReader::new(data, 8, length_bits as usize);

    let pdu_type = r.take(2)?;
    if pdu_type != 0 {
        return Err(format!("unsupported LIP PDU type {pdu_type}; only SHORT LOCATION REPORT is implemented"));
    }

    let _time_elapsed = r.take(2)?;
    let longitude_raw = r.take(25)? as u32;
    let latitude_raw = r.take(24)? as u32;
    let position_error_raw = r.take(3)? as u8;
    let velocity_raw = r.take(7)? as u8;
    let direction_raw = r.take(4)? as u8;
    let _additional_type = r.take(1)? as u8;
    let reason_or_user_data = r.take(8)? as u8;

    let longitude_signed = sign_extend(longitude_raw, 25);
    let latitude_signed = sign_extend(latitude_raw, 24);

    // ETSI LIP WGS-84 scaling.
    let longitude = longitude_signed as f64 * 180.0 / 16_777_216.0; // 2^24
    let latitude = latitude_signed as f64 * 90.0 / 8_388_608.0;     // 2^23
    let direction_deg = direction_raw as f64 * 22.5;

    if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
        return Err(format!("decoded position outside WGS-84 range: {latitude}, {longitude}"));
    }

    Ok(LipPosition {
        latitude,
        longitude,
        position_error_raw,
        velocity_raw,
        direction_raw,
        direction_deg,
        reason_or_user_data,
    })
}

fn sign_extend(value: u32, bits: u8) -> i32 {
    let sign = 1u32 << (bits - 1);
    if value & sign != 0 {
        (value as i64 - (1i64 << bits)) as i32
    } else {
        value as i32
    }
}

fn build_aprs_position(callsign: &str, pos: &LipPosition, table: char, symbol: char) -> String {
    let lat = aprs_lat(pos.latitude);
    let lon = aprs_lon(pos.longitude);

    format!(
        "{}>APRS,TCPIP*:!{}{}{}{}",
        callsign.trim().to_ascii_uppercase(),
        lat,
        table,
        lon,
        symbol
    )
}

fn aprs_lat(value: f64) -> String {
    let hemi = if value < 0.0 { 'S' } else { 'N' };
    let a = value.abs();
    let deg = a.floor() as u32;
    let min = (a - deg as f64) * 60.0;
    format!("{deg:02}{min:05.2}{hemi}")
}

fn aprs_lon(value: f64) -> String {
    let hemi = if value < 0.0 { 'W' } else { 'E' };
    let a = value.abs();
    let deg = a.floor() as u32;
    let min = (a - deg as f64) * 60.0;
    format!("{deg:03}{min:05.2}{hemi}")
}

fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join("")
}

struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
    end: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8], start: usize, end: usize) -> Self {
        Self { data, bit: start, end }
    }

    fn take(&mut self, count: usize) -> Result<u64, String> {
        if self.bit + count > self.end || self.bit + count > self.data.len() * 8 {
            return Err(format!("not enough bits while reading {count} bits"));
        }

        let mut out = 0u64;
        for _ in 0..count {
            let byte = self.data[self.bit / 8];
            let shift = 7 - (self.bit % 8);
            out = (out << 1) | ((byte >> shift) & 1) as u64;
            self.bit += 1;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_airbus_short_lip_example() {
        // Airbus example:
        // 0A 0124DA52C411E200020 (84 bits total; final nibble is significant).
        let nibbles = "0A0124DA52C411E200020";
        let mut bits = Vec::new();
        for c in nibbles.chars() {
            let v = c.to_digit(16).unwrap() as u8;
            for shift in (0..4).rev() {
                bits.push((v >> shift) & 1);
            }
        }
        let mut bytes = vec![0u8; (bits.len() + 7) / 8];
        for (i, bit) in bits.iter().enumerate() {
            bytes[i / 8] |= *bit << (7 - (i % 8));
        }

        let p = decode_short_lip(&bytes, 84).unwrap();
        assert!((p.longitude - 25.739014).abs() < 0.00001);
        assert!((p.latitude - 62.232699).abs() < 0.00001);
        assert_eq!(p.velocity_raw, 0);
        assert_eq!(p.direction_raw, 0);
    }
}
