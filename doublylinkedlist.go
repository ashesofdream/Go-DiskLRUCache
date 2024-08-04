package disklrucache

import (
	"fmt"
)

type DoublyLinkedListNode[T any] struct {
	key  string
	val  T
	prev *DoublyLinkedListNode[T]
	next *DoublyLinkedListNode[T]
}

type DoublyLinkedList[T any] struct {
	head *DoublyLinkedListNode[T]
	tail *DoublyLinkedListNode[T]
	size int
}

func NewDoublyLinkedList[T any]() *DoublyLinkedList[T] {
	return &DoublyLinkedList[T]{head: nil, tail: nil, size: 0}
}
func (dl *DoublyLinkedList[T]) Size() int {
	return dl.size
}

func (dl *DoublyLinkedList[T]) PushNode(node *DoublyLinkedListNode[T]) *DoublyLinkedListNode[T] {
	if dl.tail != nil {
		dl.tail.next = node
	}
	if dl.head == nil {
		dl.head = node
	}
	node.prev = dl.tail
	node.next = nil
	dl.tail = node
	dl.size++
	return node
}

func (dl *DoublyLinkedList[T]) Push(val T) *DoublyLinkedListNode[T] {
	newnode := &DoublyLinkedListNode[T]{val: val, prev: nil, next: nil}
	dl.PushNode(newnode)
	return newnode
}
func (dl *DoublyLinkedList[T]) PushWithKey(key string, val T) *DoublyLinkedListNode[T] {
	newnode := &DoublyLinkedListNode[T]{key: key, val: val, prev: nil, next: nil}
	dl.PushNode(newnode)
	return newnode
}

func (dl *DoublyLinkedList[T]) Pop() *DoublyLinkedListNode[T] {
	if dl.size == 0 {
		return nil
	}
	node := dl.head
	dl.head = dl.head.next
	dl.size--
	if dl.head == nil {
		dl.tail = nil
	} else {
		dl.head.prev = nil
	}
	return node
}

func (dl *DoublyLinkedList[T]) Del(node *DoublyLinkedListNode[T]) *DoublyLinkedListNode[T] {
	if node == nil {
		return nil
	}
	next := node.next
	prev := node.prev

	if next != nil {
		next.prev = prev
	}
	if prev != nil {
		prev.next = next
	}
	if node == dl.head {
		dl.head = next
	}
	if node == dl.tail {
		dl.tail = prev
	}
	dl.size--
	return node
}

func (dl *DoublyLinkedList[T]) ToString() string {
	str := ""
	node := dl.head
	for node != nil {
		str += fmt.Sprintf("%v ", node.val)
		node = node.next
	}
	return str
}
func (dl *DoublyLinkedList[T]) ToReverseString() string {
	str := ""
	node := dl.tail
	for node != nil {
		str += fmt.Sprintf("%v ", node.val)
		node = node.prev
	}
	return str
}
