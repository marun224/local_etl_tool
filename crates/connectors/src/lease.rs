//! The lease keeper (Settled decision 60): a thread that extends a queue's
//! hold on what a run received, every half-period, until the receipt is
//! settled. SQS extends a visibility timeout and Pub/Sub an ack deadline; what
//! "extend" means is the connector's, passed in as a closure.

use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

pub(crate) struct Keeper {
    stop: mpsc::Sender<()>,
    thread: JoinHandle<()>,
    /// The first extension that failed, if any.
    trouble: Arc<Mutex<Option<String>>>,
}

impl Keeper {
    /// Call `extend` every `period` until stopped. It returns what went wrong,
    /// if anything did; the first such answer is kept for [`stop`](Self::stop).
    pub(crate) fn start(
        period: Duration,
        mut extend: impl FnMut() -> Option<String> + Send + 'static,
    ) -> Keeper {
        let (stop, stopped) = mpsc::channel::<()>();
        let trouble = Arc::new(Mutex::new(None));
        let noted = trouble.clone();

        let thread = std::thread::spawn(move || {
            // Anything but a timeout -- a stop, or the receipt gone -- ends it.
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(period) {
                if let Some(failed) = extend() {
                    noted.lock().unwrap().get_or_insert(failed);
                }
            }
        });
        Keeper {
            stop,
            thread,
            trouble,
        }
    }

    /// Stop, wait for it, and say what went wrong, if anything did.
    pub(crate) fn stop(self) -> Option<String> {
        drop(self.stop);
        let _ = self.thread.join();
        self.trouble.lock().unwrap().take()
    }
}
