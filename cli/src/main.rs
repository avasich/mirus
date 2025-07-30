#![feature(normalize_lexically)]

mod app;
pub mod error;
pub mod event;
mod parser;
mod ui;

#[tokio::main]
async fn main() -> Result<(), crate::error::Error> {
    let (run, rx) = crate::app::create_app();
    let ui_handle = crate::ui::start_ui(rx);
    run.await?;
    ui_handle.await?;
    Ok(())
}
