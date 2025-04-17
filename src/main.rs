use crossbeam_channel::{unbounded, Receiver, Sender};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use log::{debug, info};
use env_logger;

#[derive(Debug, Clone)]
struct Event {
    seq: u64,
    payload: String,
}

fn receiver_thread_a(
    name: &'static str,
    rx: Receiver<Event>,
    last_processed: Arc<AtomicU64>,
    use_sync: bool,
) {
    let mut events_count: usize = 0;
    for event in rx.iter() {
        let seq = event.seq;

        if use_sync {
            last_processed.store(seq, Ordering::SeqCst);
            debug!(
                "[Receiver {}] Updated last_processed to: {} after processing event: {:?}",
                name, seq, event
            );
        } else {
            debug!("[Receiver {}] Processed event: {:?}", name, event);
        }
        events_count += 1;
    }

    info!("[Receiver {}] Done. count: {events_count}", name);
}

fn receiver_thread_b(
    name: &'static str,
    rx: Receiver<Event>,
    last_processed: Arc<AtomicU64>,
    use_sync: bool,
) {
    let mut events_count: usize = 0;
    for event in rx.iter() {
        let seq = event.seq;

        if use_sync {
            // Spin-wait until the previous sequence is processed
            while last_processed.load(Ordering::SeqCst) < seq - 1 {
                std::thread::yield_now(); // Yield to avoid busy-waiting
            }

            let current = last_processed.load(Ordering::SeqCst);
            if current < seq {
                let _ = last_processed.compare_exchange(current, seq, Ordering::SeqCst, Ordering::SeqCst);
            }

            debug!(
                "[Receiver {}] Updated last_processed to: {} after processing event: {:?}",
                name, seq, event
            );
        } else {
            debug!("[Receiver {}] Processed event: {:?}", name, event);
        }
        events_count += 1;
    }

    info!("[Receiver {}] Done, count: {events_count}.", name);
}

fn sender_thread(tx_a: Sender<Event>, tx_b: Sender<Event>) {
    let mut seq = 1;

    for i in 0..10000000 {
        let tx = if i % 2 == 0 { &tx_a } else { &tx_b };

        let event = Event {
            seq,
            payload: format!("Payload {}", seq),
        };

        tx.send(event).unwrap();
        debug!(
            "[Sender] Sent event {} to {}",
            seq,
            if i % 2 == 0 { "A" } else { "B" }
        );
        seq += 1;
    }

    drop(tx_a);
    drop(tx_b);
    info!("[Sender] Done sending events.");
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    let use_sync = args.get(1).map_or(false, |arg| arg == "sync");
    info!("Using sync: {}", use_sync);

    let start = Instant::now();

    let (tx_a, rx_a) = unbounded::<Event>();
    let (tx_b, rx_b) = unbounded::<Event>();

    let last_processed = Arc::new(AtomicU64::new(0)); // last_processed = 0

    let state_a = Arc::clone(&last_processed);
    let state_b = Arc::clone(&last_processed);

    let handle_a = thread::spawn(move || receiver_thread_a("A", rx_a, state_a, use_sync));
    let handle_b = thread::spawn(move || receiver_thread_b("B", rx_b, state_b, use_sync));
    let sender_handle = thread::spawn(move || sender_thread(tx_a, tx_b));

    sender_handle.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();
    info!("All done in {}ms", start.elapsed().as_millis());
}