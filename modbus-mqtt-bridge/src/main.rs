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
    serial_port:      String,
    baud_rate:        u32,
    mqtt_host:        String,
    mqtt_port:        u16,
    mqtt_id:          String,
    topic_cmd:        String,
    topic_rsp:        String,
    topic_status:     String,
    topic_discovered: String,
    timeout_ms:       u64,
    // Bus scan settings
    scan_on_startup:  bool,
    scan_baud_rates:  Vec<u32>,
    scan_slave_start: u8,
    scan_slave_end:   u8,
}

impl Config {
    fn from_env() -> Self {
        Self {
            serial_port:      env_str("SERIAL_PORT",       "/dev/ttyUSB0"),
            baud_rate:        env_num("BAUD_RATE",          9600),
            mqtt_host:        env_str("MQTT_HOST",          "localhost"),
            mqtt_port:        env_num("MQTT_PORT",          1883),
            mqtt_id:          env_str("MQTT_CLIENT_ID",     "modbus-bridge"),
            topic_cmd:        env_str("TOPIC_COMMAND",      "modbus/command"),
            topic_rsp:        env_str("TOPIC_RESPONSE",     "modbus/response"),
            topic_status:     env_str("TOPIC_STATUS",       "modbus/status"),
            topic_discovered: env_str("TOPIC_DISCOVERED",   "modbus/discovered"),
            timeout_ms:       env_num("TIMEOUT_MS",         1000),
            scan_on_startup:  env_bool("SCAN_ON_STARTUP",   true),
            scan_baud_rates:  env_baud_rates("SCAN_BAUD_RATES", &[9600, 4800, 19200, 38400]),
            scan_slave_start: env_num("SCAN_SLAVE_START",   1),
            scan_slave_end:   env_num("SCAN_SLAVE_END",     20),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key).as_deref() {
        Ok("true" | "1" | "yes") => true,
        Ok("false" | "0" | "no") => false,
        _ => default,
    }
}

fn env_baud_rates(key: &str, default: &[u32]) -> Vec<u32> {
    std::env::var(key)
        .ok()
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect::<Vec<u32>>())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_vec())
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
//   {"action":"scan_bus"}              — scan all baud rates and slave IDs from config
//   {"action":"scan_bus","baud_rates":[9600],"slave_id_start":1,"slave_id_end":5}
//
// Optional field "request_id" is echoed in the response for correlation.

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Cmd {
    ReadHoldingRegisters   { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadInputRegisters     { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadCoils              { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    ReadDiscreteInputs     { slave_id: u8, address: u16, count: u16, #[serde(default)] request_id: Option<String> },
    WriteSingleRegister    { slave_id: u8, address: u16, value: u16, #[serde(default)] request_id: Option<String> },
    WriteMultipleRegisters { slave_id: u8, address: u16, values: Vec<u16>, #[serde(default)] request_id: Option<String> },
    WriteSingleCoil        { slave_id: u8, address: u16, value: bool, #[serde(default)] request_id: Option<String> },
    ScanBus {
        #[serde(default = "default_scan_baud_rates")]
        baud_rates: Vec<u32>,
        #[serde(default = "default_scan_slave_start")]
        slave_id_start: u8,
        #[serde(default = "default_scan_slave_end")]
        slave_id_end: u8,
        #[serde(default)]
        request_id: Option<String>,
    },
}

fn default_scan_baud_rates() -> Vec<u32> { vec![9600, 4800, 19200, 38400] }
fn default_scan_slave_start() -> u8 { 1 }
fn default_scan_slave_end() -> u8 { 247 }

impl Cmd {
    fn request_id(&self) -> Option<&str> {
        match self {
            Self::ReadHoldingRegisters    { request_id, .. }
            | Self::ReadInputRegisters    { request_id, .. }
            | Self::ReadCoils             { request_id, .. }
            | Self::ReadDiscreteInputs    { request_id, .. }
            | Self::WriteSingleRegister   { request_id, .. }
            | Self::WriteMultipleRegisters{ request_id, .. }
            | Self::WriteSingleCoil       { request_id, .. }
            | Self::ScanBus               { request_id, .. } => request_id.as_deref(),
        }
    }

    fn action_label(&self) -> &'static str {
        match self {
            Self::ReadHoldingRegisters { .. }    => "read_holding_registers",
            Self::ReadInputRegisters { .. }      => "read_input_registers",
            Self::ReadCoils { .. }               => "read_coils",
            Self::ReadDiscreteInputs { .. }      => "read_discrete_inputs",
            Self::WriteSingleRegister { .. }     => "write_single_register",
            Self::WriteMultipleRegisters { .. }  => "write_multiple_registers",
            Self::WriteSingleCoil { .. }         => "write_single_coil",
            Self::ScanBus { .. }                 => "scan_bus",
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
        let action = cmd.action_label();
        tracing::info!(action, request_id = ?rid, "modbus serial: start");

        let result = match &cmd {
            Cmd::ScanBus { baud_rates, slave_id_start, slave_id_end, .. } => scan_bus(
                &mut *port,
                &rx,
                baud_rates,
                *slave_id_start,
                *slave_id_end,
                config.baud_rate,
                Duration::from_millis(config.timeout_ms),
            ),
            _ => run_cmd(&mut *port, &cmd),
        };

        match &result {
            Ok(_) => tracing::info!(action, request_id = ?rid, "modbus serial: OK"),
            Err(e) => tracing::warn!(
                action, request_id = ?rid, error = %e,
                "modbus serial: failed",
            ),
        }
        let _ = tx.send(result);
    }
}

/// Process any commands queued during a scan so the bridge stays responsive.
/// Restores the port to normal baud/timeout before each command, then returns
/// without restoring scan settings (caller must do that after each call).
fn service_pending_cmds(
    port: &mut dyn SerialPort,
    cmd_rx: &mpsc::Receiver<CmdPacket>,
    normal_baud: u32,
    normal_timeout: Duration,
) {
    while let Ok((cmd, tx)) = cmd_rx.try_recv() {
        if matches!(&cmd, Cmd::ScanBus { .. }) {
            let _ = tx.send(Err(anyhow!("scan already in progress, try again later")));
            continue;
        }
        port.set_baud_rate(normal_baud).ok();
        port.set_timeout(normal_timeout).ok();
        port.clear(serialport::ClearBuffer::All).ok();

        let rid = cmd.request_id();
        tracing::info!(action = %cmd.action_label(), request_id = ?rid, "modbus serial: interleaved during scan");
        let result = run_cmd(port, &cmd);
        let _ = tx.send(result);
    }
}

// ─── Bus scan ─────────────────────────────────────────────────────────────────

/// Probe each baud_rate × slave_id combination.
/// Any response (including Modbus exception) means the device is present.
/// Restores original baud rate and timeout when done.
fn scan_bus(
    port: &mut dyn SerialPort,
    cmd_rx: &mpsc::Receiver<CmdPacket>,
    baud_rates: &[u32],
    slave_id_start: u8,
    slave_id_end: u8,
    restore_baud: u32,
    restore_timeout: Duration,
) -> Result<serde_json::Value> {
    const SCAN_TIMEOUT: Duration = Duration::from_millis(300);
    let mut discovered: Vec<serde_json::Value> = Vec::new();

    for &baud in baud_rates {
        match port.set_baud_rate(baud) {
            Ok(_) => tracing::info!(
                baud,
                slave_start = slave_id_start,
                slave_end = slave_id_end,
                "scan: probing baud rate",
            ),
            Err(e) => {
                tracing::warn!(baud, error = %e, "scan: cannot set baud rate, skipping");
                continue;
            }
        }
        port.set_timeout(SCAN_TIMEOUT).ok();

        for slave_id in slave_id_start..=slave_id_end {
            // Service any commands queued while scanning; after this call the
            // port may be at restore_baud, so we re-apply scan settings.
            service_pending_cmds(port, cmd_rx, restore_baud, restore_timeout);
            port.set_baud_rate(baud).ok();
            port.set_timeout(SCAN_TIMEOUT).ok();

            port.clear(serialport::ClearBuffer::All).ok();

            let mut req = ModbusRequest::new(slave_id, ModbusProto::Rtu);
            let mut frame = Vec::new();
            if req.generate_get_coils(0, 1, &mut frame).is_err() {
                continue;
            }
            if port.write_all(&frame).is_err() {
                continue;
            }
            port.flush().ok();

            // Any 3-byte RTU header (normal or exception) means device is alive.
            let mut header = [0u8; 3];
            if port.read_exact(&mut header).is_ok() {
                let is_exception = header[1] & 0x80 != 0;
                port.clear(serialport::ClearBuffer::Input).ok();

                // Probe capabilities: which register types the device supports.
                let caps = probe_capabilities(port, slave_id);
                tracing::info!(
                    slave_id, baud, is_exception,
                    coils               = caps.coils,
                    discrete_inputs     = caps.discrete_inputs,
                    holding_registers   = caps.holding_registers,
                    input_registers     = caps.input_registers,
                    "scan: device found",
                );
                discovered.push(serde_json::json!({
                    "slave_id":           slave_id,
                    "baud_rate":          baud,
                    "coils":              caps.coils,
                    "discrete_inputs":    caps.discrete_inputs,
                    "holding_registers":  caps.holding_registers,
                    "input_registers":    caps.input_registers,
                }));
            } else {
                // Drain leftover bytes so the next probe is clean.
                port.clear(serialport::ClearBuffer::Input).ok();
            }
        }
    }

    // Restore normal operating parameters.
    port.set_baud_rate(restore_baud).ok();
    port.set_timeout(restore_timeout).ok();

    tracing::info!(found = discovered.len(), "scan: complete");
    Ok(serde_json::json!({
        "type":       "scan_result",
        "discovered": discovered,
    }))
}

// ─── Capability detection ─────────────────────────────────────────────────────

struct Capabilities {
    coils:              u16,
    discrete_inputs:    u16,
    holding_registers:  u16,
    input_registers:    u16,
}

/// For a device already known to be alive, probe all four Modbus register types
/// and return how many of each kind it supports (0 = not supported / exception).
fn probe_capabilities(port: &mut dyn SerialPort, slave_id: u8) -> Capabilities {
    // func 0x01 — coils (read/write digital output)
    // func 0x02 — discrete inputs (read-only digital input)
    // func 0x03 — holding registers (read/write 16-bit)
    // func 0x04 — input registers (read-only 16-bit, sensors)
    Capabilities {
        coils:             probe_func(port, slave_id, 0x01, 8),
        discrete_inputs:   probe_func(port, slave_id, 0x02, 8),
        holding_registers: probe_func(port, slave_id, 0x03, 4),
        input_registers:   probe_func(port, slave_id, 0x04, 4),
    }
}

/// Send one read request for `count` items of `func` code.
/// Returns `count` on a valid (non-exception) response, 0 otherwise.
fn probe_func(port: &mut dyn SerialPort, slave_id: u8, func: u8, count: u16) -> u16 {
    port.clear(serialport::ClearBuffer::All).ok();

    let mut req = ModbusRequest::new(slave_id, ModbusProto::Rtu);
    let mut frame = Vec::new();

    let built = match func {
        0x01 => req.generate_get_coils(0, count, &mut frame),
        0x02 => req.generate_get_discretes(0, count, &mut frame),
        0x03 => req.generate_get_holdings(0, count, &mut frame),
        0x04 => req.generate_get_inputs(0, count, &mut frame),
        _    => return 0,
    };

    if built.is_err() || port.write_all(&frame).is_err() {
        return 0;
    }
    port.flush().ok();

    let mut header = [0u8; 3];
    let responded = port.read_exact(&mut header).is_ok();
    port.clear(serialport::ClearBuffer::Input).ok();

    if responded && header[1] == func {
        count   // valid response for this function code
    } else {
        0       // timeout, IO error, or exception response
    }
}

// ─── Per-command execution ────────────────────────────────────────────────────

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

        Cmd::ScanBus { .. } => {
            // Handled before run_cmd is called — should never reach here.
            Err(anyhow!("internal: ScanBus must not be dispatched to run_cmd"))
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
        let mut tail = [0u8; 2];
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
        serial            = %config.serial_port,
        baud              = config.baud_rate,
        mqtt              = %format!("{}:{}", config.mqtt_host, config.mqtt_port),
        mqtt_client_id    = %config.mqtt_id,
        topic_command     = %config.topic_cmd,
        topic_response    = %config.topic_rsp,
        topic_status      = %config.topic_status,
        topic_discovered  = %config.topic_discovered,
        serial_timeout_ms = config.timeout_ms,
        scan_on_startup   = config.scan_on_startup,
        scan_baud_rates   = ?config.scan_baud_rates,
        scan_slave_start  = config.scan_slave_start,
        scan_slave_end    = config.scan_slave_end,
        "modbus-mqtt-bridge starting",
    );

    let (cmd_tx, cmd_rx) = mpsc::channel::<CmdPacket>();
    {
        let cfg = config.clone();
        std::thread::spawn(move || modbus_thread(cfg, cmd_rx));
    }

    let mut opts = MqttOptions::new(&config.mqtt_id, &config.mqtt_host, config.mqtt_port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                info!(session_present = ack.session_present, "MQTT connected → subscribing");

                let c           = client.clone();
                let topic_cmd   = config.topic_cmd.clone();
                let topic_status = config.topic_status.clone();
                let serial      = config.serial_port.clone();
                let baud        = config.baud_rate;
                tokio::spawn(async move {
                    match c.subscribe(&topic_cmd, QoS::AtLeastOnce).await {
                        Ok(()) => info!(topic = %topic_cmd, "MQTT subscribe sent"),
                        Err(e) => error!(topic = %topic_cmd, error = %e, "MQTT subscribe failed"),
                    }
                    let status = serde_json::json!({
                        "status":      "online",
                        "serial_port": serial,
                        "baud_rate":   baud,
                    });
                    if let Ok(p) = serde_json::to_vec(&status) {
                        let _ = c.publish(&topic_status, QoS::AtLeastOnce, true, p).await;
                    }
                });

                // Auto-scan on startup
                if config.scan_on_startup {
                    let cmd_tx2       = cmd_tx.clone();
                    let c2            = client.clone();
                    let t_discovered  = config.topic_discovered.clone();
                    let baud_rates    = config.scan_baud_rates.clone();
                    let slave_start   = config.scan_slave_start;
                    let slave_end     = config.scan_slave_end;

                    tokio::spawn(async move {
                        info!(
                            baud_rates  = ?baud_rates,
                            slave_start,
                            slave_end,
                            "scan: auto-scan starting",
                        );
                        let (tx, rx) = oneshot::channel();
                        let scan_cmd = Cmd::ScanBus {
                            baud_rates,
                            slave_id_start: slave_start,
                            slave_id_end:   slave_end,
                            request_id:     Some("startup-scan".to_string()),
                        };

                        if cmd_tx2.send((scan_cmd, tx)).is_err() {
                            error!("scan: failed to send to serial thread");
                            return;
                        }

                        match rx.await {
                            Ok(Ok(data)) => {
                                info!(result = %data, "scan: auto-scan complete");
                                let payload = serde_json::to_vec(&data).unwrap_or_default();
                                match c2.publish(&t_discovered, QoS::AtLeastOnce, true, payload).await {
                                    Ok(()) => info!(topic = %t_discovered, "scan: result published to MQTT"),
                                    Err(e) => error!(topic = %t_discovered, error = %e, "scan: publish failed"),
                                }
                            }
                            Ok(Err(e)) => error!(error = %e, "scan: auto-scan failed"),
                            Err(_)     => error!("scan: serial thread dropped channel"),
                        }
                    });
                }
            }

            Ok(Event::Incoming(Incoming::SubAck(sa))) => {
                for (i, code) in sa.return_codes.iter().enumerate() {
                    match code {
                        SubscribeReasonCode::Success(qos) => info!(
                            packet_id = sa.pkid, filter_index = i, ?qos,
                            "MQTT subscribe acknowledged",
                        ),
                        SubscribeReasonCode::Failure => warn!(
                            packet_id = sa.pkid, filter_index = i,
                            "MQTT subscribe rejected by broker",
                        ),
                    }
                }
            }

            Ok(Event::Incoming(Incoming::Publish(p))) => {
                let topic_in     = p.topic;
                let payload      = p.payload.to_vec();
                let cmd_tx       = cmd_tx.clone();
                let c            = client.clone();
                let t_rsp        = config.topic_rsp.clone();
                let t_discovered = config.topic_discovered.clone();

                tokio::spawn(async move {
                    info!(topic = %topic_in, payload_len = payload.len(), "MQTT command received");

                    let cmd: Cmd = match serde_json::from_slice(&payload) {
                        Ok(v)  => v,
                        Err(e) => {
                            warn!(
                                topic = %topic_in,
                                error = %e,
                                payload_preview = %String::from_utf8_lossy(&payload[..payload.len().min(256)]),
                                "command JSON parse failed",
                            );
                            publish_rsp(&c, &t_rsp, Rsp {
                                success: false, data: None,
                                error: Some(format!("parse error: {}", e)),
                                request_id: None,
                            }).await;
                            return;
                        }
                    };

                    let is_scan    = matches!(cmd, Cmd::ScanBus { .. });
                    let request_id = cmd.request_id().map(String::from);
                    info!(action = %cmd.action_label(), request_id = ?request_id, "command forwarded to Modbus thread");

                    let (tx, rx) = oneshot::channel();
                    if cmd_tx.send((cmd, tx)).is_err() {
                        error!(request_id = ?request_id, "Modbus thread channel closed");
                        return;
                    }

                    let result = rx.await.unwrap_or_else(|_| Err(anyhow!("modbus thread dropped")));
                    let rsp = match result {
                        Ok(data) => {
                            // Publish scan results to modbus/discovered so local-server
                            // picks them up the same way as the startup scan.
                            if is_scan {
                                if let Ok(p) = serde_json::to_vec(&data) {
                                    if let Err(e) = c.publish(&t_discovered, QoS::AtLeastOnce, true, p).await {
                                        error!(topic = %t_discovered, error = %e, "scan: publish to discovered failed");
                                    } else {
                                        info!(topic = %t_discovered, "scan: result published to discovered");
                                    }
                                }
                            }
                            Rsp { success: true,  data: Some(data), error: None,                request_id }
                        }
                        Err(e) => Rsp { success: false, data: None, error: Some(e.to_string()), request_id },
                    };
                    publish_rsp(&c, &t_rsp, rsp).await;
                });
            }

            Err(e) => {
                error!("MQTT error: {}. Reconnecting in 5 s…", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            Ok(Event::Incoming(other)) => {
                tracing::debug!(?other, "MQTT incoming (ignored)");
            }

            _ => {}
        }
    }
}

async fn publish_rsp(client: &AsyncClient, topic: &str, rsp: Rsp) {
    let request_id = rsp.request_id.clone();
    let success    = rsp.success;
    match serde_json::to_vec(&rsp) {
        Ok(payload) => {
            let len = payload.len();
            match client.publish(topic, QoS::AtLeastOnce, false, payload).await {
                Ok(()) => info!(topic = %topic, request_id = ?request_id, success, payload_len = len, "Modbus response published"),
                Err(e) => error!(topic = %topic, request_id = ?request_id, error = %e, "MQTT publish failed"),
            }
        }
        Err(e) => error!(request_id = ?request_id, error = %e, "failed to serialize response"),
    }
}
