use std::mem;
use std::sync::{Arc, Mutex};

use chrono::Local;
use tap::Pipe;

#[derive(Debug, Clone)]
pub struct SharedIndexState(Arc<Mutex<IndexState>>);

#[derive(Debug, Clone)]
struct IndexState {
    date: String,
}

impl SharedIndexState {
    pub fn new() -> Self {
        IndexState {
            date: "NaN".to_owned(),
        }
        .pipe(Mutex::new)
        .pipe(Arc::new)
        .pipe(SharedIndexState)
    }

    pub fn date(&self) -> String {
        self.state().date
    }

    pub fn update(&self) {
        IndexState {
            date: Local::now().to_rfc3339(),
        }
        .pipe(|s| self.set_state(s));
    }

    fn state(&self) -> IndexState {
        self.0.lock().unwrap().clone()
    }

    fn set_state(&self, state: IndexState) -> IndexState {
        self.0
            .lock()
            .unwrap()
            .pipe_deref_mut(|g| mem::replace(g, state))
    }
}
