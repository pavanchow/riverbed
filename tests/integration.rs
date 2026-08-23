use riverbed::{Broker, OverflowPolicy};

#[test]
fn message_reaches_all_subscribers_of_topic_and_none_of_another() {
    let mut broker = Broker::new(1024);
    let a = broker.subscribe("orders", 10, OverflowPolicy::RejectNew).unwrap();
    let b = broker.subscribe("orders", 10, OverflowPolicy::RejectNew).unwrap();
    let other = broker.subscribe("payments", 10, OverflowPolicy::RejectNew).unwrap();

    let delivered = broker.publish("orders", b"placed".to_vec()).unwrap();
    assert_eq!(delivered, 2);

    assert_eq!(broker.receive(a).unwrap(), Some(b"placed".to_vec()));
    assert_eq!(broker.receive(b).unwrap(), Some(b"placed".to_vec()));
    assert_eq!(broker.receive(other).unwrap(), None);
}

#[test]
fn wildcard_matching_delivers_only_to_matching_topics() {
    let mut broker = Broker::new(1024);
    let logs = broker.subscribe("logs.*", 10, OverflowPolicy::RejectNew).unwrap();
    let metrics = broker.subscribe("metrics.*", 10, OverflowPolicy::RejectNew).unwrap();

    broker.publish("logs.warn", b"w".to_vec()).unwrap();
    broker.publish("metrics.cpu", b"c".to_vec()).unwrap();
    broker.publish("logs.info", b"i".to_vec()).unwrap();

    assert_eq!(broker.receive(logs).unwrap(), Some(b"w".to_vec()));
    assert_eq!(broker.receive(logs).unwrap(), Some(b"i".to_vec()));
    assert_eq!(broker.receive(logs).unwrap(), None);

    assert_eq!(broker.receive(metrics).unwrap(), Some(b"c".to_vec()));
    assert_eq!(broker.receive(metrics).unwrap(), None);
}

#[test]
fn bounded_queue_at_capacity_drops_oldest_rather_than_growing() {
    let mut broker = Broker::new(1024);
    let s = broker.subscribe("t", 3, OverflowPolicy::DropOldest).unwrap();

    for i in 0..10 {
        broker.publish("t", vec![i]).unwrap();
    }

    assert_eq!(broker.queue_len(s).unwrap(), 3, "queue must stay at max_depth, never grow");
    assert_eq!(broker.receive(s).unwrap(), Some(vec![7]));
    assert_eq!(broker.receive(s).unwrap(), Some(vec![8]));
    assert_eq!(broker.receive(s).unwrap(), Some(vec![9]));
}

#[test]
fn bounded_queue_at_capacity_rejects_new_rather_than_growing() {
    let mut broker = Broker::new(1024);
    let s = broker.subscribe("t", 3, OverflowPolicy::RejectNew).unwrap();

    for i in 0..10 {
        broker.publish("t", vec![i]).unwrap();
    }

    assert_eq!(broker.queue_len(s).unwrap(), 3, "queue must stay at max_depth, never grow");
    assert_eq!(broker.receive(s).unwrap(), Some(vec![0]));
    assert_eq!(broker.receive(s).unwrap(), Some(vec![1]));
    assert_eq!(broker.receive(s).unwrap(), Some(vec![2]));
}

#[test]
fn counters_reflect_delivered_versus_dropped() {
    let mut broker = Broker::new(1024);
    let s = broker.subscribe("t", 2, OverflowPolicy::RejectNew).unwrap();

    for i in 0..5 {
        broker.publish("t", vec![i]).unwrap();
    }

    let stats = broker.subscriber_stats(s).unwrap();
    assert_eq!(stats.delivered, 2);
    assert_eq!(stats.dropped, 3);

    let topic_stats = broker.topic_stats("t");
    assert_eq!(topic_stats.delivered, 2);
    assert_eq!(topic_stats.dropped, 3);
}

#[test]
fn after_unsubscribe_subscriber_receives_nothing() {
    let mut broker = Broker::new(1024);
    let s = broker.subscribe("t", 10, OverflowPolicy::RejectNew).unwrap();

    broker.publish("t", b"before".to_vec()).unwrap();
    broker.unsubscribe(s).unwrap();

    let delivered = broker.publish("t", b"after".to_vec()).unwrap();
    assert_eq!(delivered, 0, "no active subscriber should receive this publish");
    assert!(broker.receive(s).is_err(), "unsubscribed id must no longer resolve");
}

#[test]
fn oversized_message_is_rejected_with_typed_error() {
    let mut broker = Broker::new(8);
    let err = broker.publish("t", b"this is definitely too long".to_vec());
    assert!(err.is_err());
}
