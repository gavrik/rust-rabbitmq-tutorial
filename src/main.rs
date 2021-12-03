use lapin::{
 Result,
};
use log::info;

mod tutorial_one;
mod tutorial_two;
mod tutorial_three;

fn main() -> Result<()> {

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    env_logger::init();

    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://guest:guest@192.168.88.200:5672".into());

    info!("TUTORIAL ONE");
    //let _r = tutorial_one::tutorial_one_send(&addr);
    //let _r = tutorial_one::tutorial_one_consume(&addr);

    info!("TUTORIAL TWO");
    //let _r = tutorial_two::tutorial_two_send(&addr);
    //let _r = tutorial_two::tutorial_two_consume(&addr);

    info!("TUTORIAL THREE");
    let _r = tutorial_three::tutorial_three_topology(&addr);
    let _r = tutorial_three::tutorial_three_send(&addr);
    let _r = tutorial_three::tutorial_three_consume(&addr);

    Ok(())

}

