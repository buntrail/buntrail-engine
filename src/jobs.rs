//! Bounded worker results. Releasing a handle never grants another worker slot until it exits.
use crate::{Result, fail};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io::Write,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
};
const MAX_JOBS: usize = 32;
const MAX_EVENTS: usize = 32;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_ITEM: usize = 64 * 1024;
#[derive(Default)]
pub(super) struct Jobs {
    registry: Mutex<Registry>,
    active: Arc<AtomicUsize>,
}
#[derive(Default)]
struct Registry {
    next: u64,
    jobs: HashMap<u64, Arc<Job>>,
}
#[derive(Default)]
struct State {
    events: VecDeque<(Value, usize)>,
    bytes: usize,
    finished: bool,
}
#[derive(Default)]
pub(super) struct Job {
    state: Mutex<State>,
    ready: Condvar,
    cancelled: Arc<AtomicBool>,
}
fn poisoned<T>(_: T) -> crate::Failure {
    fail("engine_failed", "Service job state is poisoned")
}
fn cancelled() -> crate::Failure {
    fail(
        "cancelled",
        "Service job cancelled; inspect any submitted mutation before retrying",
    )
}
struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Jobs {
    pub fn start(&self, work: impl FnOnce(&Job) -> Result<()> + Send + 'static) -> Result<u64> {
        let mut registry = self.registry.lock().map_err(poisoned)?;
        if registry.jobs.len() >= MAX_JOBS || self.active.load(Ordering::SeqCst) >= MAX_JOBS {
            return Err(fail(
                "busy",
                "Service job capacity reached; release finished jobs",
            ));
        }
        let id = registry
            .next
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Service job IDs exhausted"))?;
        registry.next = id;
        let job = Arc::new(Job::default());
        registry.jobs.insert(id, job.clone());
        self.active.fetch_add(1, Ordering::SeqCst);
        let permit = Permit(self.active.clone());
        if let Err(error) = thread::Builder::new()
            .name("buntrail-service-job".into())
            .spawn(move || {
                let _permit = permit;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(&job)))
                    .unwrap_or_else(|_| {
                        Err(fail(
                            "worker_failed",
                            "Service worker failed; inspect submitted mutations before retrying",
                        ))
                    });
                let _ = job.finish(result);
            })
        {
            registry.jobs.remove(&id);
            return Err(fail("unavailable", error.to_string()));
        }
        Ok(id)
    }
    pub fn get(&self, id: u64) -> Result<Arc<Job>> {
        self.registry
            .lock()
            .map_err(poisoned)?
            .jobs
            .get(&id)
            .cloned()
            .ok_or_else(|| fail("not_found", "Service job is no longer retained"))
    }
    pub fn release(&self, id: u64) -> Result<()> {
        let job = self
            .registry
            .lock()
            .map_err(poisoned)?
            .jobs
            .remove(&id)
            .ok_or_else(|| fail("not_found", "Service job is no longer retained"))?;
        job.cancel()
    }
}
impl Drop for Jobs {
    fn drop(&mut self) {
        for job in self
            .registry
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .jobs
            .values()
        {
            let _ = job.cancel();
        }
    }
}
struct ByteCount(usize);
impl Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_ITEM)
            .ok_or_else(|| std::io::Error::other("Service item exceeds 64 KiB"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn size(value: &Value) -> Result<usize> {
    let mut count = ByteCount(0);
    serde_json::to_writer(&mut count, value).map_err(|_| {
        fail(
            "too_large",
            "Service item exceeds 64 KiB; use a streaming operation",
        )
    })?;
    Ok(count.0)
}
impl Job {
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(poisoned)
    }
    fn enqueue<'a>(
        &self,
        mut state: MutexGuard<'a, State>,
        event: Value,
        bytes: usize,
    ) -> Result<MutexGuard<'a, State>> {
        while !state.finished
            && (state.events.len() >= MAX_EVENTS || state.bytes + bytes > MAX_BYTES)
        {
            state = self.ready.wait(state).map_err(poisoned)?;
        }
        if !state.finished {
            state.events.push_back((event, bytes));
            state.bytes += bytes;
        }
        Ok(state)
    }
    pub fn emit(&self, value: Value) -> Result<()> {
        let event = json!({"kind":"item","value":value});
        let bytes = size(&event)?;
        let state = self.enqueue(self.lock()?, event, bytes)?;
        if self.cancelled.load(Ordering::SeqCst) || state.finished {
            return Err(cancelled());
        }
        Ok(())
    }
    fn finish(&self, result: Result<()>) -> Result<()> {
        let event = match result {
            Ok(()) => json!({"kind":"finished","value":null}),
            Err(error) => {
                json!({"kind":"failed","value":{"code":error.code,"message":error.message.chars().take(4096).collect::<String>()}})
            }
        };
        let bytes = size(&event)?;
        let mut state = self.enqueue(self.lock()?, event, bytes)?;
        state.finished = true;
        self.ready.notify_all();
        Ok(())
    }
    pub fn cancel(&self) -> Result<()> {
        let mut state = self.lock()?;
        if !state.finished {
            self.cancelled.store(true, Ordering::SeqCst);
            state.events.clear();
            state.bytes = 0;
            let error = cancelled();
            let event =
                json!({"kind":"failed","value":{"code":error.code,"message":error.message}});
            let bytes = size(&event)?;
            state.events.push_back((event, bytes));
            state.bytes = bytes;
            state.finished = true;
        }
        self.ready.notify_all();
        Ok(())
    }
    pub fn poll(&self) -> Result<Value> {
        let mut state = self.lock()?;
        let events: Vec<_> = state.events.drain(..).map(|(event, _)| event).collect();
        state.bytes = 0;
        let done = state.finished;
        self.ready.notify_all();
        Ok(json!({"events":events,"done":done}))
    }
    #[cfg(unix)]
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
    /// Only workers poll service futures: some services do blocking work on their first poll.
    pub fn run<T>(
        &self,
        mut future: Pin<Box<dyn Future<Output = Result<T>> + Send + '_>>,
    ) -> Result<T> {
        struct Unpark(thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        loop {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(cancelled());
            }
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(value) => return value,
                Poll::Pending => thread::park_timeout(std::time::Duration::from_millis(50)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    #[test]
    fn backpressure_bounds_items_and_bytes_and_cancel_wakes_blocked_writer() {
        let jobs = Jobs::default();
        let emitted = Arc::new(AtomicUsize::new(0));
        let count = emitted.clone();
        let id = jobs
            .start(move |job| {
                for _ in 0..1000 {
                    job.emit(json!({"text":"x".repeat(60*1024)}))?;
                    count.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
            .unwrap();
        let job = jobs.get(id).unwrap();
        let until = Instant::now() + Duration::from_secs(3);
        while emitted.load(Ordering::SeqCst) < 16 {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(2));
        }
        std::thread::sleep(Duration::from_millis(30));
        {
            let state = job.state.lock().unwrap();
            assert!(state.events.len() <= 32);
            assert!(state.bytes <= 1024 * 1024);
        }
        assert_eq!(emitted.load(Ordering::SeqCst), 17);
        job.cancel().unwrap();
        let result = job.poll().unwrap();
        assert_eq!(result["events"].as_array().unwrap().len(), 1);
        assert_eq!(result["events"][0]["kind"], "failed");
        assert_eq!(result["done"], true);
        while jobs.active.load(Ordering::SeqCst) != 0 {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(job.poll().unwrap()["events"].as_array().unwrap().is_empty());
    }
    #[test]
    fn shutdown_retires_blocked_streams_without_waiting_for_a_consumer() {
        let jobs = Jobs::default();
        let active = jobs.active.clone();
        let id = jobs
            .start(|job| {
                for _ in 0..1000 {
                    job.emit(json!([0]))?;
                }
                Ok(())
            })
            .unwrap();
        let job = jobs.get(id).unwrap();
        drop(jobs);
        let until = Instant::now() + Duration::from_secs(3);
        while active.load(Ordering::SeqCst) != 0 {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(job.poll().unwrap()["events"][0]["kind"], "failed");
    }
    #[test]
    fn released_workers_keep_their_capacity_until_their_actual_exit() {
        let jobs = Jobs::default();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let mut ids = vec![];
        for _ in 0..32 {
            let gate = gate.clone();
            ids.push(
                jobs.start(move |_| {
                    let (lock, ready) = &*gate;
                    let mut done = lock.lock().unwrap();
                    while !*done {
                        done = ready.wait(done).unwrap();
                    }
                    Ok(())
                })
                .unwrap(),
            );
        }
        for id in ids {
            jobs.release(id).unwrap();
        }
        assert_eq!(jobs.start(|_| Ok(())).unwrap_err().code, "busy");
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
    }
}
