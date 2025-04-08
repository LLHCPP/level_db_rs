use crossbeam::channel::{unbounded, Sender};
use std::thread::{self, JoinHandle};
use tracing::error;

type Task = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct ThreadPool {
    sender: Option<Sender<Task>>,
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub(crate) fn new(size: usize) -> ThreadPool {
        let (sender, receiver) = unbounded::<Task>();
        let mut handles = Vec::with_capacity(size);

        for _ in 0..size {
            let receiver = receiver.clone();
            let handle = thread::spawn(move || {
                while let Ok(task) = receiver.recv() {
                    task();
                }
            });
            handles.push(handle);
        }

        ThreadPool {
            sender: Some(sender),
            handles,
        }
    }

    pub(crate) fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let task = Box::new(f);
        if let Some(sender) = self.sender.as_ref() {
            sender.send(task).unwrap();
        } else {
            error!("ThreadPool is dropped");
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.sender.take();
        for handle in self.handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
