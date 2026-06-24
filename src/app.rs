mod chat;
mod connect;
mod serve;
mod shell;

pub(crate) use chat::run_chat;
pub(crate) use connect::run_connect;
pub(crate) use serve::run_serve;
#[allow(unused_imports)]
pub(crate) use shell::{run_control_shell, run_tui};

fn generate_room_id() -> String {
    let mut buf = [0u8; 4];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    hex::encode(buf)
}
