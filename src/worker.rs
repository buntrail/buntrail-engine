use std::{sync::Arc, thread};
/// Only call on service workers: several current service futures block while being polled.
pub(crate) fn run<T>(mut future: buntrail_core::contracts::async_types::ServiceFuture<'_, T>) -> T {
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    loop {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park_timeout(std::time::Duration::from_millis(100)),
        }
    }
}
