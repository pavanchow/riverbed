use clap::{Parser, Subcommand};
use riverbed::{Broker, OverflowPolicy};

#[derive(Parser)]
#[command(name = "riverbed", version, about = "An in-memory publish/subscribe message broker")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a scripted scenario: subscribers on exact and wildcard topics,
    /// a normal publisher, and a slow subscriber whose bounded queue
    /// overflows and drops, then print the delivery and drop counts.
    Demo,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Demo => run_demo(),
    }
}

fn run_demo() {
    let mut broker = Broker::new(4096);

    let fast = broker
        .subscribe("logs.error", 100, OverflowPolicy::RejectNew)
        .expect("subscribe fast");
    let wildcard = broker
        .subscribe("logs.*", 100, OverflowPolicy::RejectNew)
        .expect("subscribe wildcard");
    let slow = broker
        .subscribe("logs.error", 4, OverflowPolicy::DropOldest)
        .expect("subscribe slow");

    println!("riverbed demo");
    println!("subscribers: fast={fast} (logs.error, depth 100, reject-new)");
    println!("             wildcard={wildcard} (logs.*, depth 100, reject-new)");
    println!("             slow={slow} (logs.error, depth 4, drop-oldest)  <- never drains, will overflow");
    println!();

    println!("publishing 20 messages to logs.error ...");
    for i in 0..20 {
        let body = format!("error #{i}").into_bytes();
        broker.publish("logs.error", body).expect("publish");
        // slow never calls receive, its queue fills and starts dropping.
    }

    println!("publishing 5 messages to logs.info (wildcard-only, fast/slow do not match) ...");
    for i in 0..5 {
        let body = format!("info #{i}").into_bytes();
        broker.publish("logs.info", body).expect("publish");
    }

    // fast and wildcard drain normally, so they never overflow.
    let mut fast_drained = 0;
    while broker.receive(fast).expect("receive fast").is_some() {
        fast_drained += 1;
    }
    let mut wildcard_drained = 0;
    while broker.receive(wildcard).expect("receive wildcard").is_some() {
        wildcard_drained += 1;
    }

    println!();
    println!("results");
    println!("-------");
    for (name, id) in [("fast", fast), ("wildcard", wildcard), ("slow", slow)] {
        let stats = broker.subscriber_stats(id).expect("stats");
        println!(
            "{name:<9} delivered={:<4} dropped={:<4} queue_len={}",
            stats.delivered,
            stats.dropped,
            broker.queue_len(id).expect("queue_len")
        );
    }
    println!("fast drained {fast_drained} messages, wildcard drained {wildcard_drained} messages before this point");

    let error_stats = broker.topic_stats("logs.error");
    let info_stats = broker.topic_stats("logs.info");
    println!();
    println!("topic logs.error: delivered={} dropped={}", error_stats.delivered, error_stats.dropped);
    println!("topic logs.info:  delivered={} dropped={}", info_stats.delivered, info_stats.dropped);

    println!();
    println!("unsubscribing slow ...");
    broker.unsubscribe(slow).expect("unsubscribe");
    broker.publish("logs.error", b"after unsubscribe".to_vec()).expect("publish");
    println!("slow no longer exists, message delivered only to fast and wildcard");
}
