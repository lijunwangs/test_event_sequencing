use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
struct Event {
    seq: u64,
    payload: String,
}


fn receiver_thread_a(
    name: &'static str,
    rx: Receiver<Event>,
    state: Arc<(Mutex<u64>, Condvar)>,
) {
    for event in rx.iter() {
        let seq = event.seq;

        // It's our turn
        println!("[Receiver {}] Processing event {:?}", name, event);
        let (lock, cvar) = &*state;
        let mut guard = lock.lock().unwrap();

        *guard = seq;
        cvar.notify_all();
    }

    println!("[Receiver {}] Done.", name);
}


fn receiver_thread(
    name: &'static str,
    rx: Receiver<Event>,
    state: Arc<(Mutex<u64>, Condvar)>,
) {
    for event in rx.iter() {
        let seq = event.seq;

        let (lock, cvar) = &*state;
        let mut guard = lock.lock().unwrap();

        // Wait until last_processed == seq - 1
        while *guard != seq - 1 {
            guard = cvar.wait(guard).unwrap();
        }

        // It's our turn
        println!("[Receiver {}] Processing event {:?}", name, event);
        *guard = seq;
        cvar.notify_all();
    }

    println!("[Receiver {}] Done.", name);
}

fn sender_thread(tx_a: Sender<Event>, tx_b: Sender<Event>) {
    let mut seq = 1;

    for i in 0..20 {
        let tx = if i % 2 == 0 { &tx_a } else { &tx_b };

        let event = Event {
            seq,
            payload: format!("Payload {}", seq),
        };

        println!("[Sender] Sending event {} to {}", seq, if i % 2 == 0 { "A" } else { "B" });

        tx.send(event).unwrap();
        seq += 1;

        thread::sleep(Duration::from_millis(20));
    }

    drop(tx_a);
    drop(tx_b);
}

fn main() {
    let (tx_a, rx_a) = unbounded::<Event>();
    let (tx_b, rx_b) = unbounded::<Event>();

    let shared_seq = Arc::new((Mutex::new(0u64), Condvar::new())); // last_processed = 0

    let state_a = Arc::clone(&shared_seq);
    let state_b = Arc::clone(&shared_seq);

    let handle_a = thread::spawn(move || receiver_thread("A", rx_a, state_a));
    let handle_b = thread::spawn(move || receiver_thread("B", rx_b, state_b));
    let sender_handle = thread::spawn(move || sender_thread(tx_a, tx_b));

    sender_handle.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();
}
