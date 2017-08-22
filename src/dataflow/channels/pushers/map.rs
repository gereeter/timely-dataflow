//! A wrapper which applies a function to every pushed value

use dataflow::channels::Content;
use timely_communication::{Push, PushRef};

/// A pusher that applies a function to every incoming value
pub struct MapPusher<F, P> {
    func: F,
    pusher: P
}

impl<F: Fn(D1) -> D2, D1, D2, P: Push<D2>> Push<D1> for MapPusher<F, P> {
    #[inline]
    fn push(&mut self, message: &mut Option<D1>) {
        let mut mapped = message.take().map(&self.func);
        self.pusher.push(&mut mapped);
    }
}

impl<F: Fn(&D1) -> D2, D1, D2, P: Push<D2>> PushRef<D1> for MapPusher<F, P> {
    #[inline]
    fn push_ref(&mut self, message: Option<&D1>) {
        let mut mapped = message.map(&self.func);
        self.pusher.push(&mut mapped);
    }
}

impl<F, P> MapPusher<F, P> {
    /// Creates a new pusher that will apply `func` to everything before passing it to `pusher`.
    pub fn new(func: F, pusher: P) -> MapPusher<F, P> {
        MapPusher {
            func: func,
            pusher: pusher,
        }
    }
}
