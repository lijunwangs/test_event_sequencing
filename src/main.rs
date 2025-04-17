use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

#[derive(Debug, Clone)]
struct Event {
    seq: u64,
    payload: String,
}

fn receiver_thread_a(name: &'static str, rx: Receiver<Event>, state: Arc<(Mutex<u64>, Condvar)>) {
    for event in rx.iter() {
        let seq = event.seq;

        // It's our turn
        println!("[Receiver {}] Processing {:?}", name, event);

        // just notify it.
        let mut guard = state.0.lock().unwrap();
        *guard = seq;
        // updating the last processed seq
        println!(
            "[Receiver {}] notify we are done with my event: {}, current: {}",
            name, seq, *guard
        );
        state.1.notify_all();
    }

    println!("[Receiver {}] Done.", name);
}

fn receiver_thread_b(name: &'static str, rx: Receiver<Event>, state: Arc<(Mutex<u64>, Condvar)>) {
    for event in rx.iter() {
        let seq = event.seq;

        let mut guard = state.0.lock().unwrap();

        // Wait until last_processed == seq - 1
        while *guard < seq - 1 {
            println!(
                "[Receiver {}] my event: {} waiting for event {}, current: {}",
                name,
                seq,
                seq - 1,
                *guard
            );
            guard = state.1.wait(guard).unwrap();
        }

        // It's our turn
        println!("[Receiver {}] Processing event {:?}", name, event);
        if *guard < seq {
            *guard = seq;
        }
        state.1.notify_all();
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

        println!(
            "[Sender] Sending event {} to {}",
            seq,
            if i % 2 == 0 { "A" } else { "B" }
        );

        tx.send(event).unwrap();
        seq += 1;

        //thread::sleep(Duration::from_millis(20));
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

    let handle_a = thread::spawn(move || receiver_thread_a("A", rx_a, state_a));
    let handle_b = thread::spawn(move || receiver_thread_b("B", rx_b, state_b));
    let sender_handle = thread::spawn(move || sender_thread(tx_a, tx_b));

    sender_handle.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();
}
