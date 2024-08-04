package disklrucache

type LinkedHashList[T any] struct {
	data_list DoublyLinkedList[T]
	data_map  map[string]*DoublyLinkedListNode[T]
}

func NewLinkedHashList[T any]() *LinkedHashList[T] {
	return &LinkedHashList[T]{
		data_list: *NewDoublyLinkedList[T](),
		data_map:  make(map[string]*DoublyLinkedListNode[T]),
	}
}

func (l *LinkedHashList[T]) Get(key string) *T {
	if node, ok := l.data_map[key]; ok {
		l.data_list.PushNode(l.data_list.Del(node))
		return &node.val
	}
	return nil
}

func (l *LinkedHashList[T]) Set(key string, value T) {
	val, ok := l.data_map[key]
	if ok {
		l.data_list.Del(val)
	}
	l.data_map[key] = l.data_list.PushWithKey(key, value)
}

func (l *LinkedHashList[T]) Pop() *T {
	node := l.data_list.Pop()
	delete(l.data_map, node.key)
	return &node.val
}

func (l *LinkedHashList[T]) Del(key string) *T {
	if node, ok := l.data_map[key]; ok {
		l.data_list.Del(node)
		delete(l.data_map, key)
		return &node.val
	}
	return nil
}

func (l *LinkedHashList[T]) Len() int {
	return l.data_list.Size()
}
func (l *LinkedHashList[T]) ToString() string {
	return l.data_list.ToString()
}

type LinkedHashListIterator[T any] struct {
	cur *DoublyLinkedListNode[T]
}

func (l *LinkedHashListIterator[T]) Next() bool {
	l.cur = l.cur.next
	return l.cur != nil
}

func (l *LinkedHashListIterator[T]) Value() *T {
	return &l.cur.val
}

func (l *LinkedHashList[T]) Iterator() LinkedHashListIterator[T] {
	return LinkedHashListIterator[T]{
		cur: &DoublyLinkedListNode[T]{next: l.data_list.head, prev: nil},
	}
}
