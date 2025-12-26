/*
 * Copyright (c) 2023, Hugo Meneses
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package list

type ListElement[K comparable, V any] struct {
	prev  *ListElement[K, V]
	next  *ListElement[K, V]
	Key   K
	Value V
}

type List[K comparable, V any] struct {
	head *ListElement[K, V]
	tail *ListElement[K, V]
	len  uint64
}

func New[K comparable, V any]() *List[K, V] {
	return &List[K, V]{
		head: nil,
		tail: nil,
		len:  0,
	}
}

func (l *List[K, V]) Push(key K, value V) *ListElement[K, V] {
	el := &ListElement[K, V]{
		prev:  nil,
		next:  nil,
		Key:   key,
		Value: value,
	}
	l.len++

	if l.tail == nil {
		l.head = el
		l.tail = el
		return el
	}

	el.prev = l.tail
	l.tail.next = el
	l.tail = el
	return el
}

func (l *List[K, V]) Pull() (*K, *V) {
	res := l.head
	l.head = res.next
	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}

	l.len--

	return &res.Key, &res.Value
}

func (l *List[K, V]) Len() uint64 {
	return l.len
}

func (l *List[K, V]) Peek() (*K, *V) {
	return &l.head.Key, &l.head.Value
}

func (l *List[K, V]) PeekTail() (*K, *V) {
	return &l.tail.Key, &l.tail.Value
}

func (el *ListElement[K, V]) Push(l *List[K, V]) {
	if el == l.tail {
		return
	}

	if l.head == el {
		l.head = el.next
	} else {
		el.prev.next = el.next
	}
	el.next.prev = el.prev
	el.next = nil
	el.prev = l.tail
	l.tail.next = el
	l.tail = el
}

func (el *ListElement[K, V]) Pull(l *List[K, V]) {
	if el.prev != nil {
		el.prev.next = el.next
	} else {
		l.head = el.next
	}

	if el.next != nil {
		el.next.prev = el.prev
	} else {
		l.tail = el.prev
	}

	l.len--
	el.prev = nil
	el.next = nil
}
