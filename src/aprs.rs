use crate::config::AprsConfig;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{mpsc, RwLock},
    time::sleep,
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct AprsGateway {
    users: Arc<RwLock<HashMap<u32, AprsUser>>>,
    tx: mpsc::UnboundedSender<String>,
    config: AprsConfig,
}

#[derive(Debug, Clone)]
struct AprsUser {
    callsign: String,
    first_name: String,
}

#[derive(Debug, Deserialize)]
struct UsersFile {
    users: Vec<UserRecord>,
}

#[derive(Debug, Deserialize)]
struct UserRecord {
    radio_id: u32,
    callsign: String,
    #[serde(default)]
    fname: String,
}

#[derive(Debug, Clone)]
struct LipPosition {
    latitude: f64,
    longitude: f64,
    position_error: u8,
    velocity_raw: u8,
    direction: f64,
    additional_data_type: u8,
    additional_data: u8,
}

impl AprsGateway {
    pub fn new(config: AprsConfig) -> Self {
        let users = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        let gateway = Self {
            users: users.clone(),
            tx,
            config: config.clone(),
        };

        tokio::spawn(aprs_worker(config.clone(), rx));
        tokio::spawn(users_reload_worker(
            config.users_file.clone(),
            config.users_reload_seconds,
            users,
        ));

        gateway
    }

    pub async fn handle_lip(&self, source_issi: u32, data: &[u8], length_bits: u16) {
        let raw_hex = to_hex(data);

        let user = {
            let users = self.users.read().await;
            users.get(&source_issi).cloned()
        };

        let Some(user) = user else {
            warn!(
                source_issi,
                length_bits,
                raw = %raw_hex,
                "APRS: source ISSI not found in users.json"
            );
            return;
        };

        let position = match decode_short_lip(data, length_bits) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    source_issi,
                    callsign = %user.callsign,
                    first_name = %user.first_name,
                    length_bits,
                    raw = %raw_hex,
                    error = %e,
                    "APRS: failed to decode LIP"
                );
                return;
            }
        };

        info!(
            source_issi,
            callsign = %user.callsign,
            first_name = %user.first_name,
            length_bits,
            raw = %raw_hex,
            latitude = position.latitude,
            longitude = position.longitude,
            position_error = position.position_error,
            direction = position.direction,
            velocity_raw = position.velocity_raw,
            additional_data_type = position.additional_data_type,
            additional_data = position.additional_data,
            "LIP decoded"
        );

        // Sem posição GPS utilizável: não injectar 0,0 no APRS-IS.
        if position.latitude.abs() < f64::EPSILON
            && position.longitude.abs() < f64::EPSILON
        {
            warn!(
                source_issi,
                callsign = %user.callsign,
                position_error = position.position_error,
                velocity_raw = position.velocity_raw,
                raw = %raw_hex,
                "APRS: LIP position is 0,0; packet ignored"
            );
            return;
        }

        if !(-90.0..=90.0).contains(&position.latitude)
            || !(-180.0..=180.0).contains(&position.longitude)
        {
            warn!(
                source_issi,
                callsign = %user.callsign,
                latitude = position.latitude,
                longitude = position.longitude,
                raw = %raw_hex,
                "APRS: invalid coordinates; packet ignored"
            );
            return;
        }

        let packet = format_aprs_position(
            &user.callsign,
            &self.config.login_callsign,
            position.latitude,
            position.longitude,
            self.config.symbol_table,
            self.config.symbol,
        );

        info!(
            source_issi,
            callsign = %user.callsign,
            packet = %packet,
            "APRS packet queued"
        );

        if let Err(e) = self.tx.send(packet) {
            warn!(
                source_issi,
                callsign = %user.callsign,
                error = %e,
                "APRS: worker channel closed"
            );
        }
    }
}

async fn users_reload_worker(
    path: std::path::PathBuf,
    reload_seconds: u64,
    users: Arc<RwLock<HashMap<u32, AprsUser>>>,
) {
    loop {
        match load_users(&path).await {
            Ok(new_users) => {
                let count = new_users.len();
                *users.write().await = new_users;
                info!(count, file = %path.display(), "APRS users database loaded");
            }
            Err(e) => {
                warn!(
                    file = %path.display(),
                    error = %e,
                    "APRS users database reload failed"
                );
            }
        }

        sleep(Duration::from_secs(reload_seconds.max(1))).await;
    }
}

async fn load_users(path: &Path) -> Result<HashMap<u32, AprsUser>> {
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;

    let parsed: UsersFile =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    let mut result = HashMap::new();

    for user in parsed.users {
        let callsign = user.callsign.trim().to_ascii_uppercase();

        if callsign.is_empty() {
            continue;
        }

        result.insert(
            user.radio_id,
            AprsUser {
                callsign,
                first_name: user.fname.trim().to_string(),
            },
        );
    }

    Ok(result)
}

async fn aprs_worker(config: AprsConfig, mut rx: mpsc::UnboundedReceiver<String>) {
    let address = format!("{}:{}", config.server, config.port);
    let reconnect_delay = Duration::from_secs(config.reconnect_seconds.max(1));
    let mut pending_packet: Option<String> = None;

    loop {
        info!(server = %address, "APRS: connecting to APRS-IS");

        match TcpStream::connect(&address).await {
            Ok(stream) => {
                let (read_half, mut write_half) = stream.into_split();

                tokio::spawn(async move {
                    let mut reader = BufReader::new(read_half);
                    let mut line = String::new();

                    loop {
                        line.clear();

                        match reader.read_line(&mut line).await {
                            Ok(0) => {
                                warn!("APRS: server closed read side");
                                break;
                            }
                            Ok(_) => {
                                let response = line.trim_end_matches(['\r', '\n']);

                                if response.is_empty() {
                                    continue;
                                }

                                if response.starts_with("# logresp")
                                    || response.starts_with("#login")
                                {
                                    let lower = response.to_ascii_lowercase();

                                    if lower.contains("unverified") {
                                        warn!(
                                            response = %response,
                                            "APRS-IS login UNVERIFIED"
                                        );
                                    } else if lower.contains("verified") {
                                        info!(
                                            response = %response,
                                            "APRS-IS login VERIFIED"
                                        );
                                    } else {
                                        info!(
                                            response = %response,
                                            "APRS-IS login response"
                                        );
                                    }
                                } else if response.starts_with('#') {
                                    info!(response = %response, "APRS-IS server");
                                } else {
                                    info!(response = %response, "APRS-IS received");
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "APRS: server read failed");
                                break;
                            }
                        }
                    }
                });

                let login = format!(
                    "user {} pass {} vers brew-server-aprs {}\r\n",
                    config.login_callsign,
                    config.passcode,
                    env!("CARGO_PKG_VERSION")
                );

                if let Err(e) = write_half.write_all(login.as_bytes()).await {
                    warn!(error = %e, "APRS: login write failed");
                    sleep(reconnect_delay).await;
                    continue;
                }

                if let Err(e) = write_half.flush().await {
                    warn!(error = %e, "APRS: login flush failed");
                    sleep(reconnect_delay).await;
                    continue;
                }

                info!(
                    login_callsign = %config.login_callsign,
                    server = %address,
                    "APRS: connected and login sent"
                );

                loop {
                    let packet = if let Some(packet) = pending_packet.take() {
                        packet
                    } else {
                        match rx.recv().await {
                            Some(packet) => packet,
                            None => {
                                warn!("APRS: worker channel closed");
                                return;
                            }
                        }
                    };

                    let line = format!("{packet}\r\n");

                    if let Err(e) = write_half.write_all(line.as_bytes()).await {
                        warn!(
                            error = %e,
                            packet = %packet,
                            "APRS: packet write failed; keeping packet and reconnecting"
                        );
                        pending_packet = Some(packet);
                        break;
                    }

                    if let Err(e) = write_half.flush().await {
                        warn!(
                            error = %e,
                            packet = %packet,
                            "APRS: packet flush failed; keeping packet and reconnecting"
                        );
                        pending_packet = Some(packet);
                        break;
                    }

                    info!(packet = %packet, "APRS: packet sent");
                }
            }
            Err(e) => {
                warn!(
                    server = %address,
                    error = %e,
                    "APRS: connection failed"
                );
            }
        }

        sleep(reconnect_delay).await;
    }
}

fn decode_short_lip(data: &[u8], length_bits: u16) -> Result<LipPosition> {
    if length_bits < 84 {
        return Err(anyhow!(
            "LIP payload too short: {} bits (need at least 84)",
            length_bits
        ));
    }

    if data.is_empty() || data[0] != 0x0A {
        return Err(anyhow!(
            "unexpected SDS protocol identifier: 0x{:02X}",
            data.first().copied().unwrap_or(0)
        ));
    }

    let mut bits = BitReader::new(data, length_bits as usize);

    let pid = bits.read_u32(8)? as u8;
    if pid != 0x0A {
        return Err(anyhow!("unexpected protocol identifier {}", pid));
    }

    let pdu_type = bits.read_u32(2)? as u8;
    if pdu_type != 0 {
        return Err(anyhow!(
            "unsupported LIP PDU type {} (only Short Location Report supported)",
            pdu_type
        ));
    }

    let _time_elapsed = bits.read_u32(2)? as u8;
    let lon_raw = bits.read_u32(25)?;
    let lat_raw = bits.read_u32(24)?;
    let position_error = bits.read_u32(3)? as u8;
    let velocity_raw = bits.read_u32(7)? as u8;
    let direction_raw = bits.read_u32(4)? as u8;
    let additional_data_type = bits.read_u32(1)? as u8;
    let additional_data = bits.read_u32(8)? as u8;

    let lon_signed = sign_extend(lon_raw, 25);
    let lat_signed = sign_extend(lat_raw, 24);

    let longitude = lon_signed as f64 * 180.0 / 16_777_216.0;
    let latitude = lat_signed as f64 * 90.0 / 8_388_608.0;
    let direction = direction_raw as f64 * 22.5;

    Ok(LipPosition {
        latitude,
        longitude,
        position_error,
        velocity_raw,
        direction,
        additional_data_type,
        additional_data,
    })
}

fn sign_extend(value: u32, bits: u8) -> i32 {
    let shift = 32 - bits as u32;
    ((value << shift) as i32) >> shift
}

fn format_aprs_position(
    callsign: &str,
    gateway_callsign: &str,
    latitude: f64,
    longitude: f64,
    symbol_table: char,
    symbol: char,
) -> String {
    let lat_abs = latitude.abs();
    let lat_deg = lat_abs.floor() as u32;
    let lat_min = (lat_abs - lat_deg as f64) * 60.0;
    let lat_hemi = if latitude >= 0.0 { 'N' } else { 'S' };

    let lon_abs = longitude.abs();
    let lon_deg = lon_abs.floor() as u32;
    let lon_min = (lon_abs - lon_deg as f64) * 60.0;
    let lon_hemi = if longitude >= 0.0 { 'E' } else { 'W' };

    let source = callsign.trim().to_ascii_uppercase();
    let gateway = gateway_callsign.trim().to_ascii_uppercase();

    let path = if source == gateway {
        "TCPIP*".to_string()
    } else {
        format!("qAR,{gateway}")
    };

    format!(
        "{}>APRS,{}:!{:02}{:05.2}{}{}{:03}{:05.2}{}{}",
        source,
        path,
        lat_deg,
        lat_min,
        lat_hemi,
        symbol_table,
        lon_deg,
        lon_min,
        lon_hemi,
        symbol
    )
}

fn to_hex(data: &[u8]) -> String {
    data.iter()
        .map(|b| format!("{:02X}", b))
        .collect::<Vec<_>>()
        .join("")
}

struct BitReader<'a> {
    data: &'a [u8],
    bit_len: usize,
    pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8], bit_len: usize) -> Self {
        Self {
            data,
            bit_len: bit_len.min(data.len() * 8),
            pos: 0,
        }
    }

    fn read_u32(&mut self, count: usize) -> Result<u32> {
        if count > 32 {
            return Err(anyhow!("cannot read more than 32 bits"));
        }

        if self.pos + count > self.bit_len {
            return Err(anyhow!(
                "bitstream underrun at bit {} reading {} bits (length {})",
                self.pos,
                count,
                self.bit_len
            ));
        }

        let mut value = 0u32;

        for _ in 0..count {
            let byte = self.data[self.pos / 8];
            let bit = 7 - (self.pos % 8);
            value = (value << 1) | ((byte >> bit) & 1) as u32;
            self.pos += 1;
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_airbus_lip_example() {
        let data = hex_to_bytes("0A0124DA52C411E200020");
        let pos = decode_short_lip(&data, 84).unwrap();

        assert!((pos.longitude - 25.739014).abs() < 0.001);
        assert!((pos.latitude - 62.232699).abs() < 0.001);
        assert_eq!(pos.velocity_raw, 0);
        assert_eq!(pos.direction, 0.0);
    }

    #[test]
    fn gated_position_uses_qar_gateway_path() {
        let packet =
            format_aprs_position("CT1ABC", "CT1JIB-10", 38.59095, -8.92166, '/', '>');

        assert!(packet.starts_with("CT1ABC>APRS,qAR,CT1JIB-10:!"));
    }

    #[test]
    fn own_position_uses_tcpip_path() {
        let packet =
            format_aprs_position("CT1JIB-10", "CT1JIB-10", 38.59095, -8.92166, '/', '>');

        assert!(packet.starts_with("CT1JIB-10>APRS,TCPIP*:!"));
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
