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
use crate::Refreshable;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

#[test]
fn subscribe_unsubscribe() {
    let (refreshable, mut handle) = Refreshable::<i32, ()>::new(1);
    assert_eq!(*refreshable.get(), 1);

    let value = Arc::new(AtomicI32::new(0));
    let subscription = refreshable.subscribe({
        let value = value.clone();
        move |new_value| value.store(*new_value, Ordering::SeqCst)
    });

    assert_eq!(value.load(Ordering::SeqCst), 1);

    handle.refresh(2).unwrap();
    assert_eq!(value.load(Ordering::SeqCst), 2);

    drop(subscription);

    handle.refresh(3).unwrap();
    assert_eq!(value.load(Ordering::SeqCst), 2);
}

#[test]
fn error_on_subscribe_doesnt_stay() {
    let (refreshable, mut handle) = Refreshable::new(1);

    let calls = Arc::new(AtomicI32::new(0));
    refreshable
        .try_subscribe({
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
    refreshable
        .subscribe({
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }
        })
        .leak();
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
    refreshable
        .subscribe({
            let root_value = root_value.clone();
            move |new_value| root_value.store(*new_value, Ordering::SeqCst)
        })
        .leak();

    let nested_value = Arc::new(AtomicI32::new(0));
    nested
        .subscribe({
            let nested_value = nested_value.clone();
            move |new_value| nested_value.store(*new_value, Ordering::SeqCst)
        })
        .leak();

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
        .try_subscribe(move |_| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err("boom")
            }
        })
        .unwrap()
        .leak();

    let count = AtomicI32::new(0);
    nested
        .try_subscribe(move |_| {
            if count.fetch_add(1, Ordering::SeqCst) <= 1 {
                Ok(())
            } else {
                Err("boom")
            }
        })
        .unwrap()
        .leak();

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
    let subscription = nested.subscribe({
        let value = value.clone();
        move |new_value| value.store(*new_value, Ordering::SeqCst)
    });

    drop(nested);

    handle.refresh(2).unwrap();
    assert_eq!(value.load(Ordering::SeqCst), 2);

    drop(subscription);
    assert!(refreshable.shared.callbacks.lock().is_empty());
}

#[test]
fn errors_are_stable() {
    let (refreshable, mut handle) = Refreshable::new(0);
    refreshable
        .try_subscribe(|v| {
            if *v % 2 == 0 {
                Ok(())
            } else {
                Err("value is odd")
            }
        })
        .unwrap()
        .leak();

    handle.refresh(1).err().unwrap();
    handle.refresh(1).err().unwrap();
}
