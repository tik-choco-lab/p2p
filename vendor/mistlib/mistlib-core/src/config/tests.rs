use super::{Config, SpatialPartitionType};

#[test]
fn auto_direction_threshold_is_tighter_than_legacy_default_for_26_dirs() {
    let threshold = Config::effective_direction_threshold_for(26, 0.0);
    assert!(
        threshold > 0.7,
        "auto threshold should be tighter than 0.7, got {threshold}"
    );
    assert!(
        threshold < 1.0,
        "auto threshold must remain a valid cosine, got {threshold}"
    );
}

#[test]
fn explicit_direction_threshold_override_is_preserved() {
    let threshold = Config::effective_direction_threshold_for(26, 0.82);
    assert!((threshold - 0.82).abs() < f32::EPSILON);
}

#[test]
fn auto_direction_threshold_uses_partition_direction_count() {
    let mut config = Config::new_default();
    config.dnve.spatial_partition_type = SpatialPartitionType::Icosahedron;
    config.dnve.density_resolution = 6;

    assert_eq!(
        config.effective_direction_threshold(),
        Config::effective_direction_threshold_for(20, 0.0)
    );
}
