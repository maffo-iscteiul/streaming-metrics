/*
 * Copyright (c) 2023, Hugo Meneses
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package linkedhashmap

import (
	list "github.com/xboshy/linkedhashmap/list"
)

type MapFunctions[K comparable, V any] interface {
	ExpiredHandler(key *K, value *V)
	CapacityRule(curcapacity uint64, curlen uint64, head *V, tail *V) uint64
}

type Map[K comparable, V any] struct {
	m         map[K]*list.ListElement[K, V]
	l         *list.List[K, V]
	capacity  uint64
	functions MapFunctions[K, V]
}

func New[K comparable, V any](
	capacity uint64,
	functions MapFunctions[K, V],
) *Map[K, V] {
	return &Map[K, V]{
		m:         make(map[K]*list.ListElement[K, V]),
		l:         list.New[K, V](),
		capacity:  capacity,
		functions: functions,
	}
}

func (m *Map[K, V]) clean() {
	f := m.functions.ExpiredHandler
	for m.capacity > 0 && m.l.Len() > m.capacity {
		k, v := m.pull()
		f(k, v)
	}
}

func (m *Map[K, V]) setCapacity() {
	_, headv := m.l.Peek()
	_, tailv := m.l.PeekTail()
	m.capacity = m.functions.CapacityRule(m.capacity, m.l.Len(), headv, tailv)
}

func (m *Map[K, V]) Resize(capacity uint64) {
	m.capacity = capacity
	m.clean()
}

func (m *Map[K, V]) Push(key K, value V) {
	if ptr, contains := m.m[key]; contains {
		ptr.Value = value
		ptr.Push(m.l)
		return
	}

	le := m.l.Push(key, value)
	m.m[key] = le
	m.setCapacity()
	m.clean()
}

func (m *Map[K, V]) Get(key K) *V {
	if ptr, contains := m.m[key]; contains {
		return &ptr.Value
	}

	return nil
}

func (m *Map[K, V]) pull() (*K, *V) {
	k, v := m.l.Pull()
	delete(m.m, *k)
	m.setCapacity()

	return k, v
}

func (m *Map[K, V]) Pull() (*K, *V) {
	k, v := m.pull()
	m.clean()

	return k, v
}

func (m *Map[K, V]) PullKey(key K) *V {
	if ptr, contains := m.m[key]; contains {
		delete(m.m, key)
		ptr.Pull(m.l)
		return &ptr.Value
	}

	return nil
}

func (m *Map[K, V]) Peek() (*K, *V) {
	return m.l.Peek()
}

func (m *Map[K, V]) PeekTail() (*K, *V) {
	return m.l.PeekTail()
}

func (m *Map[K, V]) Len() uint64 {
	return m.l.Len()
}
