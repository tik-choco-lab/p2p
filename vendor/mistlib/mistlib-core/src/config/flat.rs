use super::{
    Config, ConnectionMode, DensityEncoding, NodeListExchangeMode, SignalingConfig,
    SpatialPartitionType,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FlatConfig {
    signaling_url: Option<String>,
    signaling: Option<SignalingConfig>,
    max_connection_count: Option<u32>,
    connection_balancer_interval_seconds: Option<f32>,
    expire_seconds: Option<f32>,
    aoi_range: Option<f32>,
    hop_count: Option<u32>,
    force_disconnect_count: Option<u32>,
    storage_max_capacity_mb: Option<u64>,
    heartbeat_interval_seconds: Option<f32>,
    node_list_interval_seconds: Option<f32>,
    spatial_distance_layers: Option<u32>,
    spatial_density_resolution: Option<u32>,
    spatial_density_encoding: Option<String>,
    spatial_partition_type: Option<String>,
    direction_threshold: Option<f32>,
    connection_mode: Option<String>,
    node_list_exchange_mode: Option<String>,
}

impl FlatConfig {
    pub(super) fn from_config(c: &Config) -> Self {
        Self {
            signaling_url: Some(c.signaling_url.clone()),
            signaling: Some(c.signaling.clone()),
            max_connection_count: Some(c.limits.max_connection_count),
            connection_balancer_interval_seconds: Some(c.intervals.connection_balancer),
            expire_seconds: Some(c.limits.expire_node_seconds),
            aoi_range: Some(c.dnve.aoi_range),
            hop_count: Some(c.limits.hop_count),
            force_disconnect_count: Some(c.limits.force_disconnect_count),
            storage_max_capacity_mb: Some(c.storage.max_capacity_mb),
            heartbeat_interval_seconds: Some(c.intervals.heartbeat),
            node_list_interval_seconds: Some(c.intervals.node_list),
            spatial_distance_layers: Some(c.dnve.distance_layers),
            spatial_density_resolution: Some(c.dnve.density_resolution),
            spatial_density_encoding: Some(c.dnve.density_encoding.as_str().to_owned()),
            spatial_partition_type: Some(c.dnve.spatial_partition_type.as_str().to_owned()),
            direction_threshold: Some(c.dnve.direction_threshold),
            connection_mode: Some(c.dnve.connection_mode.as_str().to_owned()),
            node_list_exchange_mode: Some(c.dnve.node_list_exchange_mode.as_str().to_owned()),
        }
    }

    pub(super) fn apply_to(self, c: &mut Config) -> bool {
        if let Some(v) = self.signaling_url {
            c.signaling_url = v;
        }
        if let Some(v) = self.signaling {
            c.signaling = v;
        }
        if let Some(v) = self.max_connection_count {
            c.limits.max_connection_count = v;
        }
        if let Some(v) = self.connection_balancer_interval_seconds {
            c.intervals.connection_balancer = v;
        }
        if let Some(v) = self.expire_seconds {
            c.limits.expire_node_seconds = v;
        }
        if let Some(v) = self.aoi_range {
            c.dnve.aoi_range = v;
        }
        if let Some(v) = self.hop_count {
            c.limits.hop_count = v;
        }
        if let Some(v) = self.force_disconnect_count {
            c.limits.force_disconnect_count = v;
        }
        if let Some(v) = self.storage_max_capacity_mb {
            c.storage.max_capacity_mb = v;
        }
        if let Some(v) = self.heartbeat_interval_seconds {
            c.intervals.heartbeat = v;
        }
        if let Some(v) = self.node_list_interval_seconds {
            c.intervals.node_list = v;
        }
        if let Some(v) = self.spatial_distance_layers {
            c.dnve.distance_layers = v;
        }
        if let Some(v) = self.spatial_density_resolution {
            c.dnve.density_resolution = v;
        }
        if let Some(v) = self.spatial_density_encoding {
            if let Some(parsed) = DensityEncoding::parse(&v) {
                c.dnve.density_encoding = parsed;
            }
        }
        if let Some(v) = self.spatial_partition_type {
            if let Some(parsed) = SpatialPartitionType::parse(&v) {
                c.dnve.spatial_partition_type = parsed;
            }
        }
        if let Some(v) = self.direction_threshold {
            c.dnve.direction_threshold = v;
        }
        if let Some(v) = self.connection_mode {
            if let Some(parsed) = ConnectionMode::parse(&v) {
                c.dnve.connection_mode = parsed;
            }
        }
        if let Some(v) = self.node_list_exchange_mode {
            if let Some(parsed) = NodeListExchangeMode::parse(&v) {
                c.dnve.node_list_exchange_mode = parsed;
            }
        }
        c.normalize_legacy_signaling();
        true
    }
}
