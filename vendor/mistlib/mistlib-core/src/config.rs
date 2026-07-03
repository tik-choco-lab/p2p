mod enums;
mod flat;
mod signaling;

#[cfg(test)]
mod tests;

pub use enums::{ConnectionMode, DensityEncoding, NodeListExchangeMode, SpatialPartitionType};
use flat::FlatConfig;
use serde::{Deserialize, Serialize};
pub use signaling::{
    NostrSignalingConfig, SignalingConfig, SignalingMode, DEFAULT_NOSTR_RELAY_LIST_URL,
};
use std::f32::consts::PI;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub signaling_url: String,
    #[serde(default)]
    pub signaling: SignalingConfig,
    pub limits: LimitsConfig,
    pub dnve: DnveConfig,
    pub intervals: IntervalsConfig,
    pub webrtc: WebRtcConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LimitsConfig {
    pub max_connection_count: u32,
    pub expire_node_seconds: f32,
    pub hop_count: u32,
    pub reserved_connection_count: u32,
    pub force_disconnect_count: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DnveConfig {
    pub density_max_range: f32,
    pub distance_layers: u32,
    pub density_resolution: u32,
    #[serde(default)]
    pub density_encoding: DensityEncoding,
    #[serde(default)]
    pub spatial_partition_type: SpatialPartitionType,
    pub direction_threshold: f32,
    pub aoi_range: f32,
    #[serde(default)]
    pub connection_mode: ConnectionMode,
    #[serde(default)]
    pub node_list_exchange_mode: NodeListExchangeMode,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IntervalsConfig {
    pub connection_balancer: f32,
    pub heartbeat: f32,
    pub node_list: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebRtcConfig {
    pub ice_servers: Vec<IceServer>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StorageConfig {
    pub max_capacity_mb: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_capacity_mb: 8 * 1024,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

impl Config {
    const AUTO_DIRECTION_THRESHOLD_SENTINEL: f32 = 0.0;

    pub fn new_default() -> Self {
        Self {
            signaling_url: "wss://rtc.tik-choco.com/signaling".to_string(),
            signaling: SignalingConfig::default(),
            limits: LimitsConfig {
                max_connection_count: 30,
                expire_node_seconds: 10.0,
                hop_count: 2,
                reserved_connection_count: 1,
                force_disconnect_count: 0,
            },
            dnve: DnveConfig {
                density_max_range: 64.0,
                distance_layers: 1,
                density_resolution: 6,
                density_encoding: DensityEncoding::Byte,
                spatial_partition_type: SpatialPartitionType::Dodecahedron,
                direction_threshold: Self::AUTO_DIRECTION_THRESHOLD_SENTINEL,
                aoi_range: 10.0,
                connection_mode: ConnectionMode::NodeListAoiGuard,
                node_list_exchange_mode: NodeListExchangeMode::Pull,
            },
            intervals: IntervalsConfig {
                connection_balancer: 2.0,
                heartbeat: 1.0,
                node_list: 2.0,
            },
            webrtc: WebRtcConfig {
                ice_servers: vec![IceServer {
                    urls: vec!["stun:stun.l.google.com:19302".to_string()],
                    username: None,
                    credential: None,
                }],
            },
            storage: StorageConfig::default(),
        }
    }

    pub fn update_from_json(&mut self, json_str: &str) -> bool {
        if let Ok(mut new_config) = serde_json::from_str::<Config>(json_str) {
            new_config.normalize_legacy_signaling();
            if !new_config.validate_signaling() {
                return false;
            }
            *self = new_config;
            return true;
        }

        if let Ok(flat) = serde_json::from_str::<FlatConfig>(json_str) {
            let mut next = self.clone();
            if !flat.apply_to(&mut next) || !next.validate_signaling() {
                return false;
            }
            *self = next;
            return true;
        }

        false
    }

    pub fn use_websocket_signaling_url(&mut self, signaling_url: String) {
        self.signaling_url = signaling_url;
        self.signaling = SignalingConfig::websocket();
    }

    pub fn validate_signaling(&self) -> bool {
        match self.signaling.mode {
            SignalingMode::WebSocket => true,
            SignalingMode::Nostr => self
                .signaling
                .nostr
                .as_ref()
                .is_some_and(NostrSignalingConfig::validate),
        }
    }

    fn normalize_legacy_signaling(&mut self) {
        if self.signaling.mode == SignalingMode::WebSocket {
            self.signaling.nostr = None;
        }
    }

    pub fn effective_direction_threshold(&self) -> f32 {
        Self::effective_direction_threshold_for(
            self.dnve
                .spatial_partition_type
                .direction_count(self.dnve.density_resolution),
            self.dnve.direction_threshold,
        )
    }

    pub fn effective_direction_threshold_for(
        density_resolution: u32,
        configured_threshold: f32,
    ) -> f32 {
        if (0.0..=1.0).contains(&configured_threshold)
            && configured_threshold > Self::AUTO_DIRECTION_THRESHOLD_SENTINEL
        {
            return configured_threshold;
        }

        let direction_count = density_resolution.max(1) as f32;
        let spherical_cap_area = 4.0 * PI / direction_count;
        let cap_cos = (1.0 - spherical_cap_area / (2.0 * PI)).clamp(-1.0, 1.0);
        let cap_half_angle = cap_cos.acos();

        // Shrink the ideal equal-area cone slightly so neighboring directions overlap less.
        let tuned_half_angle = (cap_half_angle * 0.9).clamp(0.0, PI);
        tuned_half_angle.cos().clamp(0.0, 0.999_999)
    }

    pub fn to_json_string(&self) -> String {
        let flat = FlatConfig::from_config(self);
        serde_json::to_string(&flat).unwrap_or_else(|_| "{}".to_string())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new_default()
    }
}
