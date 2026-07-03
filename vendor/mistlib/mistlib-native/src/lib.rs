pub mod app;
pub mod config;
pub mod engine;
pub mod error;
pub mod events;
pub mod ffi;
pub mod layers;
pub mod logging;
pub mod mem;
pub mod runtime;
pub mod signaling;
pub mod stats;
pub mod storage;
pub mod transports;

pub use app::{
    clear_raw_handler, get_config, get_connected_nodes, get_connected_nodes_async,
    get_connection_state, get_connection_state_async, get_connection_state_value,
    get_connection_state_value_async, get_stats, init, init_and_join, init_with_config, join_room,
    leave_room, register_event_callback, register_log_callback, register_raw_handler, send_message,
    send_message_direct, set_config, shutdown, try_send_message, update_position,
    DELIVERY_RELIABLE, DELIVERY_UNRELIABLE, DELIVERY_UNRELIABLE_ORDERED,
};
pub use error::{MistError, Result};
pub use events::{
    EventCallback, RustEventCallback, EVENT_ALL_CONNECTIONS_LOST, EVENT_AOI_ENTERED,
    EVENT_AOI_LEFT, EVENT_AOI_NODES, EVENT_JOIN, EVENT_LEAVE, EVENT_NEIGHBORS,
    EVENT_NODE_POSITION_UPDATED, EVENT_OVERLAY, EVENT_RAW,
};
pub use layers::native_l1;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
