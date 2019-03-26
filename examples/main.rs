use crossbeam_utils::thread;

use pikmin_aggregator::Aggregator;

fn main() {
    pretty_env_logger::init_timed();

    let exchanges = vec!["bffx", "liquid", "mex"];
    let url = "mysql://root:hoge@127.0.0.1:3306/trades";

    thread::scope(|s| {
        for exchange in &exchanges {
            s.spawn(move |_| {
                let prep = Aggregator::new(url, exchange);
                prep.aggregate();
            });
        }
    })
    .unwrap();
}
