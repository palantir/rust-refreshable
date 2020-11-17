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
//!
//! A `Refreshable` provides access to both the current value and also ways to be notified of changes made in the
//! future. Users can *subscribe* to the refreshable, registering a callback which is invoked whenever the value
//! changes. Additionally, users can *map* a refreshable of one type to a refreshable of another type, with the new
//! refreshable being updated based on changes to the original refreshable. For example, a caching component of a
//! service may map a `Refreshable<ServiceConfiguration>` down to a `Refreshable<CacheConfiguration>` so it can
//! subscribe specifically to only the configuration changes that matter to it.
//!
//! A `Subscription` is returned when subscribing to a refreshable which acts as a guard type, unregistering the
//! subscription when dropped. If you intend the subscription to last for the lifetime of the refreshable, you can use
//! the `Subscription::leak` method to allow the `Subscription` to fall out of scope without unregistering.
//!
//! A `RefreshHandle` is returned when creating a new `Refreshable` which is used to update its value. Subscriptions
//! are fallible, and all errors encountered when running subscriptions in response to an update are reported through
//! the `RefreshHandle::refresh` method.
//!
//! # Examples
//!
//! ```
//! use refreshable::Refreshable;
//!
//! #[derive(PartialEq)]
//! struct ServiceConfiguration {
//!     cache: CacheConfiguration,
//!     some_other_thing: u32,
//! }
//!
//! #[derive(PartialEq, Clone)]
//! struct CacheConfiguration {
//!     size: usize,
//! }
//!
//! let initial_config = ServiceConfiguration {
//!     cache: CacheConfiguration {
//!         size: 10,
//!     },
//!     some_other_thing: 5,
//! };
//! let (refreshable, mut handle) = Refreshable::new(initial_config);
//!
//! let cache_refreshable = refreshable.map(|config| config.cache.clone());
//!
//! let subscription = cache_refreshable.subscribe(|cache| {
//!     if cache.size == 0 {
//!         Err("cache size must be positive")
//!     } else {
//!         println!("new cache size is {}", cache.size);
//!         Ok(())
//!     }
//! }).unwrap();
//!
//! let new_config = ServiceConfiguration {
//!     cache: CacheConfiguration {
//!         size: 20,
//!     },
//!     some_other_thing: 5,
//! };
//! // "new cache size is 20" is printed.
//! handle.refresh(new_config).unwrap();
//!
//! let new_config = ServiceConfiguration {
//!     cache: CacheConfiguration {
//!         size: 20,
//!     },
//!     some_other_thing: 10,
//! };
//! // nothing is printed since the cache configuration did not change.
//! handle.refresh(new_config).unwrap();
//!
//! drop(subscription);
//! let new_config = ServiceConfiguration {
//!     cache: CacheConfiguration {
//!         size: 0,
//!     },
//!     some_other_thing: 10,
//! };
//! // nothing is printed since the the cache subscription was dropped.
//! handle.refresh(new_config).unwrap();
//! ```
#![doc(html_root_url = "https://docs.rs/refreshable/1")]
#![warn(clippy::all, missing_docs)]

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(test)]
mod test;

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
    #[allow(clippy::type_complexity)]
    callbacks: Mutex<Arc<HashMap<u64, Arc<Callback<T, E>>>>>,
}

/// A wrapper around a live-refreshable value.
pub struct Refreshable<T, E> {
    shared: Arc<Shared<T, E>>,
    next_id: AtomicU64,
    // This is used to unsubscribe a mapped refreshable from its parent refreshable. A copy of the Arc is held in the
    // refreshable itself, along with every subscription of the mapped refreshable. The inner dyn Drop is a Subscription
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
            _p: PhantomData,
        }
    }

    /// Subscribes to the refreshable.
    ///
    /// The callback will be invoked every time the refreshable's value changes, and is also called synchronously when
    /// this method is called with the current value. If the callback returns `Ok`, a `Subscription` object is returned
    /// that will unsubscribe from the refreshable when it drops. If the callback returns `Err`, this method will return
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
            live: true,
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
        refreshable.cleanup = Some(Arc::new(subscription));
        refreshable
    }
}

/// A subscription to a `Refreshable` value.
///
/// The associated subscription is unregistered when this value is dropped, unless the `Subscription::leak` method is
/// used.
#[must_use = "the associated subscription is unregistered when this value is dropped"]
pub struct Subscription<T, E> {
    shared: Arc<Shared<T, E>>,
    id: u64,
    live: bool,
}

impl<T, E> Drop for Subscription<T, E> {
    fn drop(&mut self) {
        if self.live {
            Arc::make_mut(&mut *self.shared.callbacks.lock()).remove(&self.id);
        }
    }
}

impl<T, E> Subscription<T, E> {
    /// Destroys the guard without unregistering its associated subscription.
    pub fn leak(mut self) {
        self.live = false;
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
    inner: arc_swap::Guard<Arc<T>>,
    // the arc_swap guard doesn't borrow from its ArcSwap, but we don't want to expose that fact in the public API
    _p: PhantomData<&'a ()>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &*self.inner
    }
}
