//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::rc::Rc;
use std::cell::RefCell;

use dataflow::channels::Content;
use abomonation::Abomonation;

use timely_communication::{Push, PushRef};

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct Tee<T: 'static, D: 'static> {
    buffer: Vec<D>,
    ref_outputs: Rc<RefCell<Vec<Box<PushRef<(T, Content<D>)>>>>>,
    outputs: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>,
}

impl<T: Clone+'static, D: Abomonation+Clone+'static> Push<(T, Content<D>)> for Tee<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        for ref_pusher in self.ref_outputs.borrow_mut().iter_mut() {
            ref_pusher.push_ref(message.as_ref());
        }
        if let Some((ref time, ref mut data)) = *message {
            let mut pushers = self.outputs.borrow_mut();
            for index in 0..pushers.len() {
                if index < pushers.len() - 1 {
                    // TODO : was `push_all`, but is now `extend`, slow.
                    self.buffer.extend_from_slice(data);
                    Content::push_at(&mut self.buffer, (*time).clone(), &mut pushers[index]);
                }
                else {
                    Content::push_at(data, (*time).clone(), &mut pushers[index]);
                }
            }
        }
        else {
            for pusher in self.outputs.borrow_mut().iter_mut() {
                pusher.push(&mut None);
            }
        }
    }
}

impl<T, D> Tee<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (Tee<T, D>, TeeHelper<T, D>) {
        let ref_outputs = Rc::new(RefCell::new(Vec::new()));
        let outputs = Rc::new(RefCell::new(Vec::new()));
        let port = Tee {
            buffer: Vec::with_capacity(Content::<D>::default_length()),
            ref_outputs: ref_outputs.clone(),
            outputs: outputs.clone(),
        };

        (port, TeeHelper { ref_outputs: ref_outputs, outputs: outputs })
    }
}

impl<T, D> Clone for Tee<T, D> {
    fn clone(&self) -> Tee<T, D> {
        Tee {
            buffer: Vec::with_capacity(self.buffer.capacity()),
            ref_outputs: self.ref_outputs.clone(),
            outputs: self.outputs.clone(),
        }
    }
}


/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D> {
    ref_outputs: Rc<RefCell<Vec<Box<PushRef<(T, Content<D>)>>>>>,
    outputs: Rc<RefCell<Vec<Box<Push<(T, Content<D>)>>>>>,
}

impl<T, D> TeeHelper<T, D> {
    /// Adds a new `PushRef` implementor to the list of recipients shared with a `Stream`.
    pub fn add_ref_pusher<P: PushRef<(T, Content<D>)>+'static>(&self, pusher: P) {
        self.ref_outputs.borrow_mut().push(Box::new(pusher));
    }
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<(T, Content<D>)>+'static>(&self, pusher: P) {
        self.outputs.borrow_mut().push(Box::new(pusher));
    }
}

// TODO : Implemented on behalf of example_static::Stream; check if truly needed.
impl<T, D> Clone for TeeHelper<T, D> {
    fn clone(&self) -> TeeHelper<T, D> {
        TeeHelper {
            ref_outputs: self.ref_outputs.clone(),
            outputs: self.outputs.clone(),
        }
    }
}
