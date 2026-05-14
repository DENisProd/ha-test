use std::{sync::mpsc, time::Duration};

use anyhow::{anyhow, Context, Result};
use rmodbus::{client::ModbusRequest, ModbusProto};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, SubscribeReasonCode};
use serde::{Deserialize, Serialize};
use serialport::SerialPort;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    serial_port:   String,
    baud_rate:     u32,
    mqtt_host:     String,
    mqtt_port:     u16,
    mqtt_id:       String,
    topic_cmd:     String,
    topic_rsp:     String,
    topic_status:  String,
    timeout_ms:    u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            serial_port:  env_str("SERIAL_PORT",  "/dev/ttyUSB0"),
            baud_rate:    env_num("BAUD_RATE",     9600),
            mqtt_host:    env_str("MQTT_HOST",     "localhost"),
            mqtt_port:    env_num("MQTT_PORT",     1883),
            mqtt_id:      env_str("MQTT_CLIENT_ID","modbus-bridge"),
            topic_cmd:    env_str("TOPIC_COMMAND", "modbus/command"),
            topic_rsp:    env_str("TOPIC_RESPONSE","modbus/response"),
            topic_status: env_str("TOPIC_STATUS",  "modbus/status"),
            timeout_ms:   env_num("TIMEOUT_MS",    1000),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ─── MQTT command protocol ────────────────────────────────────────────────────
//
// Publish JSON to modbus/command:
//
//   {"action":"read_holding_registers","slave_id":1,"address":0,"count":10}
//   {"action":"read_input_registers",  "slave_id":1,"address":0,"count":4}
//   {"action":"read_coils",            "slave_id":1,"address":0,"count":8}
//   {"action":"read_discrete_inputs",  "slave_id":1,"address":0,"count":8}
//   {"action":"write_single_register", "slave_id":1,"address":0,"value":42}
//   {"action":"write_multiple_registers","slave_id":1,"address":0,"values":[1,2,3]}
//   {"action":"write_single_coil",     "slave_id":1,"address":0,"value":true}
//
// Optional field "request_id" is echoed in the response for correlation.

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Cmd {
    ReadHoldingRegisters  { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadInputRegisters    { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadCoils             { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadDiscreteInputs    { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    WriteSingleRegister   { slave_id: u8, address: u16, value: u16, #[serde(default)] request_id: Option<String> },
    WriteMultipleRegisters{ slave_id: u8, address: u16, values: Vec<u16>, #[serde(default)] request_id: Option<String> },
    WriteSingleCoil       { slave_id: u8, address: u16, value: bool, #[serde(default)] request_id: Option<String> },
}

impl Cmd {
    fn request_id(&self) -> Option<&str> {
        match self {
            Self::ReadHoldingRegisters   { request_id, .. }
            | Self::ReadInputRegisters   { request_id, .. }
            | Self::ReadCoils            { request_id, .. }
            | Self::ReadDiscreteInputs   { request_id, .. }
            | Self::WriteSingleRegister  { request_id, .. }
            | Self::WriteMultipleRegisters{request_id, .. }
            | Self::WriteSingleCoil      { request_id, .. } => request_id.as_deref(),
        }
    }

    fn action_label(&self) -> &'static str {
        match self {
            Self::ReadHoldingRegisters { .. } => "read_holding_registers",
            Self::ReadInputRegisters { .. } => "read_input_registers",
            Self::ReadCoils { .. } => "read_coils",
            Self::ReadDiscreteInputs { .. } => "read_discrete_inputs",
            Self::WriteSingleRegister { .. } => "write_single_register",
            Self::WriteMultipleRegisters { .. } => "write_multiple_registers",
            Self::WriteSingleCoil { .. } => "write_single_coil",
        }
    }
}

#[derive(Debug, Serialize)]
struct Rsp {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")] data:       Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")] error:      Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] request_id: Option<String>,
}

// ─── Modbus serial thread ─────────────────────────────────────────────────────

type CmdPacket = (Cmd, oneshot::Sender<Result<serde_json::Value>>);

fn modbus_thread(config: Config, rx: mpsc::Receiver<CmdPacket>) {
    let mut port: Box<dyn SerialPort> = loop {
        match serialport::new(&config.serial_port, config.baud_rate)
            .data_bits(serialport::DataBits::Eight)
            .parity(serialport::Parity::None)
            .stop_bits(serialport::StopBits::One)
            .timeout(Duration::from_millis(config.timeout_ms))
            .open()
        {
            Ok(p) => break p,
            Err(e) => {
                error!("Cannot open {}: {}. Retry in 5 s", config.serial_port, e);
                std::thread::sleep(Duration::from_secs(5));
            }
        }
    };
    info!("Serial {} @ {} baud", config.serial_port, config.baud_rate);

    while let Ok((cmd, tx)) = rx.recv() {
        let rid = cmd.request_id();
        tracing::info!(
            action = %cmd.action_label(),
            request_id = ?rid,
            "modbus serial: start",
        );
        let result = run_cmd(&mut *port, &cmd);
        match &result {
            Ok(_) => tracing::info!(action = %cmd.action_label(), request_id = ?rid, "modbus serial: OK"),
            Err(e) => tracing::warn!(
                action = %cmd.action_label(),
                request_id = ?rid,
                error = %e,
                "modbus serial: failed",
            ),
        }
        let _ = tx.send(result);
    }
}

fn run_cmd(port: &mut dyn SerialPort, cmd: &Cmd) -> Result<serde_json::Value> {
    // Flush stale bytes from previous transaction.
    port.clear(serialport::ClearBuffer::All).ok();

    match cmd {
        Cmd::ReadHoldingRegisters { slave_id, address, count, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_get_holdings(*address, *count, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x03)?;
            let mut vals = Vec::new();
            req.parse_u16(&resp, &mut vals).map_err(|e| anyhow!("parse: {:?}", e))?;
            Ok(serde_json::json!({ "type":"holding_registers", "slave_id":slave_id, "address":address, "values":vals }))
        }

        Cmd::ReadInputRegisters { slave_id, address, count, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_get_inputs(*address, *count, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x04)?;
            let mut vals = Vec::new();
            req.parse_u16(&resp, &mut vals).map_err(|e| anyhow!("parse: {:?}", e))?;
            Ok(serde_json::json!({ "type":"input_registers", "slave_id":slave_id, "address":address, "values":vals }))
        }

        Cmd::ReadCoils { slave_id, address, count, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_get_coils(*address, *count, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x01)?;
            let mut bits = Vec::new();
            req.parse_bool(&resp, &mut bits).map_err(|e| anyhow!("parse: {:?}", e))?;
            bits.truncate(*count as usize);
            Ok(serde_json::json!({ "type":"coils", "slave_id":slave_id, "address":address, "values":bits }))
        }

        Cmd::ReadDiscreteInputs { slave_id, address, count, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_get_discretes(*address, *count, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x02)?;
            let mut bits = Vec::new();
            req.parse_bool(&resp, &mut bits).map_err(|e| anyhow!("parse: {:?}", e))?;
            bits.truncate(*count as usize);
            Ok(serde_json::json!({ "type":"discrete_inputs", "slave_id":slave_id, "address":address, "values":bits }))
        }

        Cmd::WriteSingleRegister { slave_id, address, value, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_set_holding(*address, *value, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x06)?;
            req.parse_ok(&resp).map_err(|e| anyhow!("parse: {:?}", e))?;
            Ok(serde_json::json!({ "type":"write_register", "slave_id":slave_id, "address":address, "value":value }))
        }

        Cmd::WriteMultipleRegisters { slave_id, address, values, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_set_holdings_bulk(*address, values, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x10)?;
            req.parse_ok(&resp).map_err(|e| anyhow!("parse: {:?}", e))?;
            Ok(serde_json::json!({ "type":"write_registers", "slave_id":slave_id, "address":address, "count":values.len() }))
        }

        Cmd::WriteSingleCoil { slave_id, address, value, .. } => {
            let mut req = ModbusRequest::new(*slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            req.generate_set_coil(*address, *value, &mut frame)
                .map_err(|e| anyhow!("build frame: {:?}", e))?;
            port.write_all(&frame).context("serial write")?;
            port.flush().ok();

            let resp = read_rtu_response(port, 0x05)?;
            req.parse_ok(&resp).map_err(|e| anyhow!("parse: {:?}", e))?;
            Ok(serde_json::json!({ "type":"write_coil", "slave_id":slave_id, "address":address, "value":value }))
        }
    }
}

/// Read one complete Modbus RTU response frame from the serial port.
///
/// RTU read response:  [slave][func][byte_count][data…][crc_lo][crc_hi]
/// RTU write response: [slave][func][addr_hi][addr_lo][val_hi][val_lo][crc_lo][crc_hi]  (8 bytes)
/// RTU exception:      [slave][func|0x80][exc_code][crc_lo][crc_hi]                     (5 bytes)
fn read_rtu_response(port: &mut dyn SerialPort, expected_func: u8) -> Result<Vec<u8>> {
    let mut header = [0u8; 3];
    port.read_exact(&mut header).context("reading RTU header")?;

    let func = header[1];

    // Exception: high bit of function code set
    if func & 0x80 != 0 {
        let mut tail = [0u8; 2]; // [exc_code, crc_lo] — crc_hi follows
        let mut crc_hi = [0u8; 1];
        port.read_exact(&mut tail).ok();
        port.read_exact(&mut crc_hi).ok();
        return Err(anyhow!(
            "Modbus exception (func={:#04x}, code={})",
            func & 0x7F,
            header[2]
        ));
    }

    if func != expected_func {
        return Err(anyhow!(
            "Unexpected function code: got {:#04x}, expected {:#04x}",
            func, expected_func
        ));
    }

    // For read functions header[2] = byte count of the data payload.
    // For write functions the total frame is 8 bytes (3 header + 5 remaining).
    let tail_len: usize = match func {
        0x01 | 0x02 | 0x03 | 0x04 => header[2] as usize + 2, // data + 2 CRC
        0x05 | 0x06 | 0x0F | 0x10 => 5,                      // 5 bytes complete 8-byte response
        _ => return Err(anyhow!("Unknown function code {:#04x}", func)),
    };

    let mut tail = vec![0u8; tail_len];
    port.read_exact(&mut tail).context("reading RTU body")?;

    let mut frame = header.to_vec();
    frame.extend_from_slice(&tail);
    Ok(frame)
}

// ─── Async MQTT loop ──────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "modbus_mqtt_bridge=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();

    info!(
        serial = %config.serial_port,
        baud = config.baud_rate,
        mqtt = %format!("{}:{}", config.mqtt_host, config.mqtt_port),
        mqtt_client_id = %config.mqtt_id,
        topic_command = %config.topic_cmd,
        topic_response = %config.topic_rsp,
        topic_status = %config.topic_status,
        serial_timeout_ms = config.timeout_ms,
        "modbus-mqtt-bridge starting (check topics match local-server)",
    );

    // Spawn blocking modbus thread
    let (cmd_tx, cmd_rx) = mpsc::channel::<CmdPacket>();
    {
        let cfg = config.clone();
        std::thread::spawn(move || modbus_thread(cfg, cmd_rx));
    }

    // MQTT client
    let mut opts = MqttOptions::new(&config.mqtt_id, &config.mqtt_host, config.mqtt_port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                info!(
                    session_present = ack.session_present,
                    "MQTT connected → subscribing",
                );
                let c = client.clone();
                let topic   = config.topic_cmd.clone();
                let t_status = config.topic_status.clone();
                let serial  = config.serial_port.clone();
                let baud    = config.baud_rate;
                tokio::spawn(async move {
                    match c.subscribe(&topic, QoS::AtLeastOnce).await {
                        Ok(()) => info!(topic = %topic, "MQTT subscribe sent (wait for SubAck)"),
                        Err(e) => error!(topic = %topic, error = %e, "MQTT subscribe failed — commands will never arrive"),
                    }
                    let status = serde_json::json!({
                        "status": "online",
                        "serial_port": serial,
                        "baud_rate": baud,
                    });
                    match c
                        .publish(
                            &t_status,
                            QoS::AtLeastOnce,
                            true,
                            serde_json::to_vec(&status).unwrap_or_default(),
                        )
                        .await
                    {
                        Ok(()) => info!(topic = %t_status, "MQTT status published"),
                        Err(e) => error!(topic = %t_status, error = %e, "MQTT status publish failed"),
                    }
                });
            }

            Ok(Event::Incoming(Incoming::SubAck(sa))) => {
                for (i, code) in sa.return_codes.iter().enumerate() {
                    match code {
                        SubscribeReasonCode::Success(qos) => info!(
                            packet_id = sa.pkid,
                            filter_index = i,
                            ?qos,
                            "MQTT subscribe acknowledged",
                        ),
                        SubscribeReasonCode::Failure => warn!(
                            packet_id = sa.pkid,
                            filter_index = i,
                            "MQTT subscribe rejected by broker — command topic will not receive messages",
                        ),
                    }
                }
            }

            Ok(Event::Incoming(Incoming::Publish(p))) => {
                let topic_in = p.topic;
                let payload   = p.payload.to_vec();
                let cmd_tx    = cmd_tx.clone();
                let c         = client.clone();
                let t_rsp     = config.topic_rsp.clone();

                tokio::spawn(async move {
                    info!(
                        topic = %topic_in,
                        payload_len = payload.len(),
                        "MQTT publish received (command)",
                    );

                    let cmd: Cmd = match serde_json::from_slice(&payload) {
                        Ok(v)  => v,
                        Err(e) => {
                            warn!(
                                topic = %topic_in,
                                error = %e,
                                payload_preview = %String::from_utf8_lossy(&payload[..payload.len().min(256)]),
                                "command JSON parse failed — publishing error response",
                            );
                            publish_rsp(&c, &t_rsp, Rsp {
                                success: false, data: None,
                                error: Some(format!("parse error: {}", e)),
                                request_id: None,
                            }).await;
                            return;
                        }
                    };

                    let request_id = cmd.request_id().map(String::from);
                    info!(
                        action = %cmd.action_label(),
                        request_id = ?request_id,
                        topic = %topic_in,
                        "command accepted, forwarding to Modbus thread",
                    );

                    let (tx, rx)   = oneshot::channel();

                    if cmd_tx.send((cmd, tx)).is_err() {
                        error!(
                            request_id = ?request_id,
                            "Modbus thread channel closed — cannot execute (bridge serial thread dead?)",
                        );
                        return;
                    }

                    let result = rx.await.unwrap_or_else(|_| Err(anyhow!("modbus thread dropped")));

                    let rsp = match result {
                        Ok(data) => Rsp { success: true,  data: Some(data), error: None,          request_id },
                        Err(e)   => Rsp { success: false, data: None,       error: Some(e.to_string()), request_id },
                    };
                    publish_rsp(&c, &t_rsp, rsp).await;
                });
            }

            Err(e) => {
                error!("MQTT error: {}. Reconnecting in 5 s…", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            Ok(Event::Incoming(other)) => {
                tracing::debug!(?other, "MQTT incoming (ignored by command handler)");
            }

            _ => {}
        }
    }
}

async fn publish_rsp(client: &AsyncClient, topic: &str, rsp: Rsp) {
    let request_id = rsp.request_id.clone();
    let success = rsp.success;
    match serde_json::to_vec(&rsp) {
        Ok(payload) => {
            let len = payload.len();
            match client.publish(topic, QoS::AtLeastOnce, false, payload).await {
                Ok(()) => {
                    info!(
                        topic = %topic,
                        request_id = ?request_id,
                        success,
                        payload_len = len,
                        "published Modbus response to MQTT",
                    );
                }
                Err(e) => {
                    error!(
                        topic = %topic,
                        request_id = ?request_id,
                        success,
                        error = %e,
                        "MQTT publish of response failed — local-server will see timeout",
                    );
                }
            }
        }
        Err(e) => error!(
            request_id = ?request_id,
            error = %e,
            "failed to serialize Modbus response JSON",
        ),
    }
}
