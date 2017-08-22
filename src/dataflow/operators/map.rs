//! Extension methods for `Stream` based on record-by-record transformation.

use Data;
use dataflow::{Stream, Scope};
use dataflow::channels::pact::Pipeline;
use dataflow::channels::Content;
use dataflow::channels::pushers::{MapPusher, Tee};
use dataflow::operators::generic::unary::Unary;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
    /// Looks at each element of the stream by reference and yields a new element.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_ref(|&x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_ref<D2: Data, L: Fn(&D)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
    /// Updates each element of the stream and yields the element, re-using memory where possible.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_in_place(|x| *x += 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D>;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<I: IntoIterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data;
    /// Consumes each message sent down the stream and yields a new, transformed message at the same time.
    ///
    /// This method is meant mostly for internal use.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_batch(|mut data| {
    ///                for datum in &mut data {
    ///                    *datum += 1;
    ///                }
    ///                data
    ///            })
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_batch<D2: Data, L: Fn(Content<D>)->Content<D2>+'static>(&self, logic: L) -> Stream<S, D2>;
    /// Looks at each message sent down the stream by reference and yields a new, transformed message at the same time.
    ///
    /// This method is meant mostly for internal use.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::channels::Content;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_batch_ref(|data| {
    ///                let mut mapped = data.iter()
    ///                                     .map(|&x| x + 1)
    ///                                     .collect();
    ///                Content::from_typed(&mut mapped)
    ///            })
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_batch_ref<D2: Data, L: Fn(&Content<D>)->Content<D2>+'static>(&self, logic: L) -> Stream<S, D2>;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: Fn(D)->D2+'static>(&self, logic: L) -> Stream<S, D2> {
        self.map_batch(move |mut data| {
            let mut mapped: Vec<_> = data.replace_with(Vec::new())
                                         .into_iter()
                                         .map(&logic)
                                         .collect();
            Content::from_typed(&mut mapped)
        })
    }
    fn map_ref<D2: Data, L: Fn(&D)->D2+'static>(&self, logic: L) -> Stream<S, D2> {
        self.map_batch_ref(move |data| {
            let mut mapped: Vec<_> = data.iter()
                                         .map(&logic)
                                         .collect();
            Content::from_typed(&mut mapped)
        })
    }
    fn map_in_place<L: Fn(&mut D)+'static>(&self, logic: L) -> Stream<S, D> {
        self.map_batch(move |mut data| {
            for datum in data.iter_mut() {
                logic(datum);
            }
            data
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data {
        self.unary_stream(Pipeline, "FlatMap", move |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).flat_map(|x| logic(x).into_iter()));
            });
        })
    }
    // fn filter_map<D2: Data, L: Fn(D)->Option<D2>+'static>(&self, logic: L) -> Stream<S, D2> {
    //     self.unary_stream(Pipeline, "FilterMap", move |input, output| {
    //         while let Some((time, data)) = input.next() {
    //             output.session(time).give_iterator(data.drain(..).filter_map(|x| logic(x)));
    //         }
    //     })
    // }
    fn map_batch<D2: Data, L: Fn(Content<D>)->Content<D2>+'static>(&self, logic: L) -> Stream<S, D2> {
        let (targets, registrar) = Tee::<S::Timestamp,D2>::new();
        self.add_pusher(MapPusher::new(move |(time, data)| (time, logic(data)), targets));

        Stream::new(
            *self.name(),
            registrar,
            self.scope()
        )
    }
    fn map_batch_ref<D2: Data, L: Fn(&Content<D>)->Content<D2>+'static>(&self, logic: L) -> Stream<S, D2> {
        let (targets, registrar) = Tee::<S::Timestamp,D2>::new();
        self.add_ref_pusher(MapPusher::new(move |&(ref time, ref data): &(S::Timestamp, _)| {
            (time.clone(), logic(data))
        }, targets));

        Stream::new(
            *self.name(),
            registrar,
            self.scope()
        )
    }
}
