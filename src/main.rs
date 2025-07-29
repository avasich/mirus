#[tokio::main]
async fn main() -> Result<(), mirus::Error> {
    let (run, rx) = mirus::create_app();
    mirus::start_ui(rx);
    run.await
}
