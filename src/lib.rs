// Copyright 2020 Palantir Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! A simple wrapper around a value that changes over time.
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

struct RawCallback<F: ?Sized> {
    _cleanup: Option<Arc<dyn Drop + Sync + Send>>,
    // We need to run failing callbacks again on a refresh even if the value didn't change. Otherwise, you can "lose"
    // errors when the refreshable update to the same value.
    ok: AtomicBool,
    callback: F,
}

type Callback<T, E> = RawCallback<dyn Fn(&T, &mut Vec<E>) + Sync + Send>;

struct Shared<T, E> {
    value: ArcSwap<T>,
    // This guards subscription registration against concurrent refreshes. If that happened, the subscription callback
    // could miss updates that happen after the subscription is called with the "current" value but before it is
    // inserted in the callbacks map.
    update_lock: Mutex<()>,
    callbacks: Mutex<Arc<HashMap<u64, Arc<Callback<T, E>>>>>,
}

/// A wrapper around a live-refreshable value.
///
/// It allows users to look at the current value, register callbacks that are invoked when the value changes, and to
/// create new refreshables by applying a mapping function (e.g. to go from a `Refreshable<MyConfiguration>` to a
/// `Refreshable<MySubcomponentConfiguration>`.
///
/// It is additionally parameterized by an error type which allows subscriptions to report errors when the value is
/// refreshed.
pub struct Refreshable<T, E> {
    shared: Arc<Shared<T, E>>,
    next_id: AtomicU64,
    // This is used to unsubscribe a mapped refreshable from its parent refreshable. A copy of the Arc is held in the
    // refreshable itself, along with every subscription of the mapped refreshable. The inner dyn Drop is a MapCleanup
    // type.
    cleanup: Option<Arc<dyn Drop + Sync + Send>>,
}

impl<T, E> Refreshable<T, E>
where
    T: PartialEq + 'static + Sync + Send,
    E: 'static,
{
    /// Creates a new `Refreshable` with an initial value, returning it along with a `RefreshHandle` used to update it
    /// with new values.
    pub fn new(value: T) -> (Refreshable<T, E>, RefreshHandle<T, E>) {
        let shared = Arc::new(Shared {
            value: ArcSwap::new(Arc::new(value)),
            update_lock: Mutex::new(()),
            callbacks: Mutex::new(Arc::new(HashMap::new())),
        });

        (
            Refreshable {
                shared: shared.clone(),
                next_id: AtomicU64::new(0),
                cleanup: None,
            },
            RefreshHandle { shared },
        )
    }

    /// Returns a guard type providing access to a snapshot of the refreshable's current value.
    #[inline]
    pub fn get(&self) -> Guard<'_, T> {
        Guard {
            inner: self.shared.value.load(),
        }
    }

    /// Subscribes to the refreshable.
    ///
    /// The callback will be invoked every time the refreshable's value changes, and is also called synchronously when
    /// this method is called with the current value. If the callback returns `Ok`, a `Subscription` object is returned
    /// that can be used to unsubscribe from the refreshable. If the callback returns `Err`, this method will return
    /// the error and the callback will *not* be invoked on updates to the value.
    pub fn subscribe<F>(&self, callback: F) -> Result<Subscription<T, E>, E>
    where
        F: Fn(&T) -> Result<(), E> + 'static + Sync + Send,
    {
        let _guard = self.shared.update_lock.lock();
        callback(&self.get())?;

        let subscription = self.subscribe_raw(move |value, errors| {
            if let Err(e) = callback(value) {
                errors.push(e);
            }
        });

        Ok(subscription)
    }

    /// Subscribes to the refreshable with an infallible callback.
    ///
    /// This is a convenience method to simplify subscription when the callback can never fail.
    pub fn subscribe_ok<F>(&self, callback: F) -> Subscription<T, E>
    where
        F: Fn(&T) + 'static + Sync + Send,
    {
        self.subscribe(move |value| {
            callback(value);
            Ok(())
        })
        .ok()
        .unwrap()
    }

    fn subscribe_raw<F>(&self, callback: F) -> Subscription<T, E>
    where
        F: Fn(&T, &mut Vec<E>) + 'static + Sync + Send,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let callback = Arc::new(RawCallback {
            _cleanup: self.cleanup.clone(),
            ok: AtomicBool::new(true),
            callback,
        });
        Arc::make_mut(&mut *self.shared.callbacks.lock()).insert(id, callback);

        Subscription {
            shared: self.shared.clone(),
            id,
        }
    }

    /// Creates a new refreshable from this one by applying a mapping function to the value.
    ///
    /// This can be used to narrow the scope of the refreshable value. Updates to the initial refreshable value will
    /// propagate to the mapped refreshable value, but the mapped refreshable's subscriptions will only be invoked if
    /// the mapped value actually changed.
    pub fn map<F, R>(&self, map: F) -> Refreshable<R, E>
    where
        F: Fn(&T) -> R + 'static + Sync + Send,
        R: PartialEq + 'static + Sync + Send,
    {
        let _guard = self.shared.update_lock.lock();
        let (mut refreshable, handle) = Refreshable::new(map(&self.get()));
        let subscription =
            self.subscribe_raw(move |value, errors| handle.refresh_raw(map(value), errors));
        refreshable.cleanup = Some(Arc::new(MapCleanup(subscription)));
        refreshable
    }
}

/// A subscription to a `Refreshable` value.
pub struct Subscription<T, E> {
    shared: Arc<Shared<T, E>>,
    id: u64,
}

impl<T, E> Subscription<T, E> {
    /// Unsubscribes from the refreshable.
    ///
    /// The subscription's callback will be removed from the refreshable's state, and will no longer be called when the
    /// refreshable's value changes.
    pub fn unsubscribe(&self) {
        Arc::make_mut(&mut *self.shared.callbacks.lock()).remove(&self.id);
    }
}

/// A handle that can update the value associated with a refreshable.
pub struct RefreshHandle<T, E> {
    shared: Arc<Shared<T, E>>,
}

impl<T, E> RefreshHandle<T, E>
where
    T: PartialEq + 'static + Sync + Send,
{
    /// Updates the refreshable's value.
    ///
    /// If the new value is equal to the refreshable's current value, the method returns immediately. Otherwise, it
    /// runs all registered subscriptions, collecting any errors and returning them all when finished.
    // NB: It's important that this takes &mut self. That way, all the way down through the tree of mapped refreshables,
    // we don't need to worry about concurrent refreshes. refresh_raw below only takes &self to work more easily with
    // the map implementation, but that's only triggered by a call to refresh at the root refreshable.
    pub fn refresh(&mut self, new_value: T) -> Result<(), Vec<E>> {
        let mut errors = vec![];

        self.refresh_raw(new_value, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn refresh_raw(&self, new_value: T, errors: &mut Vec<E>) {
        // We could avoid updating the inner value when it hasn't changed but the complexity doesn't seem worth it.
        let value_changed = new_value != **self.shared.value.load();

        let guard = self.shared.update_lock.lock();
        self.shared.value.store(Arc::new(new_value));
        let value = self.shared.value.load();
        let callbacks = self.shared.callbacks.lock().clone();
        drop(guard);

        for callback in callbacks.values() {
            if value_changed || !callback.ok.load(Ordering::SeqCst) {
                let nerrors = errors.len();
                (callback.callback)(&value, errors);
                callback.ok.store(errors.len() == nerrors, Ordering::SeqCst);
            }
        }
    }
}

/// A guard type providing access to a snapshot of a refreshable's current value.
pub struct Guard<'a, T> {
    inner: arc_swap::Guard<'a, Arc<T>>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &*self.inner
    }
}

struct MapCleanup<T, E>(Subscription<T, E>);

impl<T, E> Drop for MapCleanup<T, E> {
    fn drop(&mut self) {
        self.0.unsubscribe();
    }
}

#[cfg(test)]
mod test {
    use crate::Refreshable;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    #[test]
    fn subscribe_unsubscribe() {
        let (refreshable, mut handle) = Refreshable::<i32, ()>::new(1);
        assert_eq!(*refreshable.get(), 1);

        let value = Arc::new(AtomicI32::new(0));
        let subscription = refreshable.subscribe_ok({
            let value = value.clone();
            move |new_value| value.store(*new_value, Ordering::SeqCst)
        });

        assert_eq!(value.load(Ordering::SeqCst), 1);

        handle.refresh(2).unwrap();
        assert_eq!(value.load(Ordering::SeqCst), 2);

        subscription.unsubscribe();

        handle.refresh(3).unwrap();
        assert_eq!(value.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn error_on_subscribe_doesnt_stay() {
        let (refreshable, mut handle) = Refreshable::new(1);

        let calls = Arc::new(AtomicI32::new(0));
        refreshable
            .subscribe({
                let calls = calls.clone();
                move |_| {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err("bang")
                    } else {
                        Ok(())
                    }
                }
            })
            .err()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        handle.refresh(2).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn equal_refresh_is_noop() {
        let (refreshable, mut handle) = Refreshable::<i32, ()>::new(1);

        let calls = Arc::new(AtomicI32::new(0));
        refreshable.subscribe_ok({
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        handle.refresh(1).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn map_basics() {
        let (refreshable, mut handle) = Refreshable::<i32, ()>::new(1);

        let nested = refreshable.map(|value| *value * 2);
        assert_eq!(*nested.get(), 2);

        let root_value = Arc::new(AtomicI32::new(0));
        refreshable.subscribe_ok({
            let root_value = root_value.clone();
            move |new_value| root_value.store(*new_value, Ordering::SeqCst)
        });

        let nested_value = Arc::new(AtomicI32::new(0));
        nested.subscribe_ok({
            let nested_value = nested_value.clone();
            move |new_value| nested_value.store(*new_value, Ordering::SeqCst)
        });

        handle.refresh(3).unwrap();
        assert_eq!(root_value.load(Ordering::SeqCst), 3);
        assert_eq!(nested_value.load(Ordering::SeqCst), 6);
        assert_eq!(*refreshable.get(), 3);
        assert_eq!(*nested.get(), 6);
    }

    #[test]
    fn map_error_propagation() {
        let (refreshable, mut handle) = Refreshable::new(1);

        let nested = refreshable.map(|value| *value * 2);

        let count = AtomicI32::new(0);
        refreshable
            .subscribe(move |_| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err("boom")
                }
            })
            .unwrap();

        let count = AtomicI32::new(0);
        nested
            .subscribe(move |_| {
                if count.fetch_add(1, Ordering::SeqCst) <= 1 {
                    Ok(())
                } else {
                    Err("boom")
                }
            })
            .unwrap();

        let errors = handle.refresh(2).err().unwrap();
        assert_eq!(errors.len(), 1);

        let errors = handle.refresh(1).err().unwrap();
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn map_callback_cleanup() {
        let (refreshable, mut handle) = Refreshable::<i32, ()>::new(1);
        refreshable.map(|v| *v);
        assert!(refreshable.shared.callbacks.lock().is_empty());

        let nested = refreshable.map(|v| *v);

        let value = Arc::new(AtomicI32::new(0));
        let subscription = nested.subscribe_ok({
            let value = value.clone();
            move |new_value| value.store(*new_value, Ordering::SeqCst)
        });

        drop(nested);

        handle.refresh(2).unwrap();
        assert_eq!(value.load(Ordering::SeqCst), 2);

        subscription.unsubscribe();
        assert!(refreshable.shared.callbacks.lock().is_empty());
    }

    #[test]
    fn errors_are_stable() {
        let (refreshable, mut handle) = Refreshable::new(0);
        refreshable
            .subscribe(|v| {
                if *v % 2 == 0 {
                    Ok(())
                } else {
                    Err("value is odd")
                }
            })
            .unwrap();

        handle.refresh(1).err().unwrap();
        handle.refresh(1).err().unwrap();
    }
}
