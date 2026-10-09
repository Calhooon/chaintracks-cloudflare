use worker::*;

/// Log to the worker console, or to stderr on a host build: the `cargo test`
/// harness drives the real sync and storage paths (bsv-low M19B-G2), and the
/// wasm-bindgen console import aborts when called off-wasm. Every log line on
/// a path the host harness can reach goes through these two.
#[cfg(target_arch = "wasm32")]
macro_rules! log {
    ($($t:tt)*) => {
        ::worker::console_log!($($t)*)
    };
}
#[cfg(not(target_arch = "wasm32"))]
macro_rules! log {
    ($($t:tt)*) => {
        eprintln!($($t)*)
    };
}
#[cfg(target_arch = "wasm32")]
macro_rules! log_error {
    ($($t:tt)*) => {
        ::worker::console_error!($($t)*)
    };
}
#[cfg(not(target_arch = "wasm32"))]
macro_rules! log_error {
    ($($t:tt)*) => {
        eprintln!($($t)*)
    };
}

#[cfg(test)]
mod canonicalize_tests;
#[cfg(test)]
mod chain_event_tests;
mod consensus;
#[cfg(test)]
mod courier_tests;
mod couriers;
mod d1;
mod events;
#[cfg(test)]
mod host_harness;
#[cfg(test)]
mod pow_tests;
mod r2;
#[cfg(test)]
mod reorg_producer_tests;
#[cfg(test)]
mod retarget_tests;
mod routes;
#[cfg(test)]
mod rule28_tests;
#[cfg(test)]
mod statement_pins;
mod storage;
#[cfg(test)]
mod storage_host_tests;
mod sync;
mod types;
mod woc;

/// Chaintracks ; BSV block header tracking on Cloudflare Workers.
///
/// Replaces the Node.js chaintracks-server with a Rust WASM worker.
/// Uses D1 for header storage, R2 for bulk header CDN files,
/// and cron triggers for WhatsOnChain polling.

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    routes::handle_request(req, &env).await
}

#[event(scheduled)]
async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    console_error_panic_hook::set_once();
    if let Err(e) = sync::poll_for_new_blocks(&env).await {
        console_log!("Cron sync error: {e:?}");
    }
}
