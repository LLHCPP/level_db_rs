use crossbeam::channel::{unbounded, Sender};
use std::thread::{self, JoinHandle};

type Task = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct ThreadPool {
    sender: Sender<Task>,
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

        ThreadPool { sender, handles }
    }

    pub(crate) fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let task = Box::new(f);
        self.sender.send(task).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            handle.join().unwrap();
        }
    }
}

fn main() {
    let pool = ThreadPool::new(2);
    pool.execute(|| println!("Task 1"));
    pool.execute(|| println!("Task 2"));
    thread::sleep(std::time::Duration::from_millis(100));
}
