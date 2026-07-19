use worker::*;

mod d1;
mod r2;
mod routes;
mod selfpop;
mod storage;
mod sync;
mod types;
mod woc;

/// Chaintracks — BSV block header tracking on Cloudflare Workers.
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
