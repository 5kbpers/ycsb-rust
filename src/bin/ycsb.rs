use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

use anyhow::Result;
use futures::future::try_join_all;
use structopt::StructOpt;
use tokio::{runtime, task};

use ycsb::{
    core::{
        client::Client,
        measurement::{Histogram, Measurement},
        properties::Properties,
        workload::CoreWorkload,
    },
    db::create_db,
};

#[derive(StructOpt)]
struct Ycsb {
    #[structopt(short, long)]
    threads: usize,
    #[structopt(short, long)]
    db: String,
    #[structopt(short, long)]
    config: String,
    #[structopt(subcommand)]
    cmd: Command,
}
#[derive(StructOpt, Clone, Copy)]
enum Command {
    Load,
    Run,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Ycsb::from_args();
    let threads = opt.threads;

    let rt = runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(threads)
        .build()?;

    let config = File::open(&opt.config).expect("cannot open config file");
    let config_reader = BufReader::new(config);
    let props = Properties::load(config_reader).expect("load properties failed");
    let db = create_db(&opt.db).expect("create db failed");

    let histogram = Histogram::new(1024);

    let handles: Vec<task::JoinHandle<_>> = match opt.cmd {
        Command::Load => (0..threads)
            .map(|_| {
                let workload = CoreWorkload::new(&props).expect("load workload failed");
                let client = Client::new(db.clone(), workload);
                let props = props.clone();
                let histogram = histogram.clone();
                let record_count = props.get_record_count();
                rt.spawn(async move {
                    (0..record_count / threads as u64).for_each(|_| {
                        let start = Instant::now();
                        let _ = client.do_insert();
                        histogram.measure(start.elapsed());
                    })
                })
            })
            .collect(),
        Command::Run => (0..threads)
            .map(|_| {
                let workload = CoreWorkload::new(&props).expect("load workload failed");
                let client = Client::new(db.clone(), workload);
                let props = props.clone();
                let histogram = histogram.clone();
                let op_count = props.get_operation_count();
                rt.spawn(async move {
                    (0..op_count / threads as u64).for_each(|_| {
                        let start = Instant::now();
                        let _ = client.do_transaction();
                        histogram.measure(start.elapsed());
                    })
                })
            })
            .collect(),
    };

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));

    tokio::select! {
        _ = tick.tick() => {
            println!("{}", histogram.summary());
        }
        _ = try_join_all(handles) => {
            println!("{}", histogram.summary());
            println!("Test exited");
        },
    };

    Ok(())
}
