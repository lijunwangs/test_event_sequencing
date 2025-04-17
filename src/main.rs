use crossbeam_channel::{unbounded, Receiver, Sender};
use std::env;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;
use log::{debug, info};
use env_logger;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Event {
    seq: u64,
    payload: String,
}

fn receiver_thread_a(
    name: &'static str,
    rx: Receiver<Event>,
    state: Arc<(Mutex<u64>, Condvar)>,
    use_sync: bool,
) {
    for event in rx.iter() {
        let seq = event.seq;

        if use_sync {
            let mut guard = state.0.lock().unwrap();
            *guard = seq;
            state.1.notify_all();
            debug!(
                "[Receiver {}] Notified after processing event: {}, current: {}",
                name, seq, *guard
            );
        } else {
            debug!("[Receiver {}] Processed event: {:?}", name, event);
        }
    }

    info!("[Receiver {}] Done.", name);
}

fn receiver_thread_b(
    name: &'static str,
    rx: Receiver<Event>,
    state: Arc<(Mutex<u64>, Condvar)>,
    use_sync: bool,
) {
    for event in rx.iter() {
        let seq = event.seq;

        if use_sync {
            let mut guard = state.0.lock().unwrap();

            while *guard < seq - 1 {
                debug!(
                    "[Receiver {}] Waiting for event {}, current: {}",
                    name,
                    seq - 1,
                    *guard
                );
                guard = state.1.wait(guard).unwrap();
            }

            if *guard < seq {
                *guard = seq;
            }
            state.1.notify_all();
            debug!(
                "[Receiver {}] Notified after processing event: {}, current: {}",
                name, seq, *guard
            );
        } else {
            debug!("[Receiver {}] Processed event: {:?}", name, event);
        }
    }

    info!("[Receiver {}] Done.", name);
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

    let shared_seq = Arc::new((Mutex::new(0u64), Condvar::new())); // last_processed = 0

    let state_a = Arc::clone(&shared_seq);
    let state_b = Arc::clone(&shared_seq);

    let handle_a = thread::spawn(move || receiver_thread_a("A", rx_a, state_a, use_sync));
    let handle_b = thread::spawn(move || receiver_thread_b("B", rx_b, state_b, use_sync));
    let sender_handle = thread::spawn(move || sender_thread(tx_a, tx_b));

    sender_handle.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();
    info!("All done in {}ms", start.elapsed().as_millis());
}