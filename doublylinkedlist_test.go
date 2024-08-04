package disklrucache

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestPushPop(t *testing.T) {
	fmt.Println("TestPushPop")
	var push_case = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	var push_node = []*DoublyLinkedListNode[int]{}
	var del_case = []int{1, 3, 5, 7, 9}
	dll := NewDoublyLinkedList[int]()
	for _, val := range push_case {
		push_node = append(push_node, dll.Push(val))
	}
	if dll.ToString() != "1 2 3 4 5 6 7 8 9 " {
		t.Errorf("ToString() should be 1 2 3 4 5 6 7 8 9, but got %s", dll.ToString())
	}
	for _, val := range del_case {
		dll.Del(push_node[val-1])
	}
	if dll.Size() != len(push_case)-len(del_case) {
		t.Errorf("Size() should be %d, but got %d", len(push_case)-len(del_case), dll.Size())
	}
	if dll.ToString() != "2 4 6 8 " {
		t.Errorf("ToString() should be 2 4 6 8, but got %s", dll.ToString())
	}
	reveseString := dll.ToReverseString()
	reveseString_arr := []rune(reveseString)
	for i := 0; i < len(reveseString)/2; i++ {
		tmp := reveseString_arr[i]
		reveseString_arr[i] = reveseString_arr[len(reveseString)-i-1]
		reveseString_arr[len(reveseString)-i-1] = tmp
	}
	reveseString = string(reveseString_arr)
	if strings.Compare(strings.TrimSpace(reveseString), strings.TrimSpace(dll.ToString())) != 0 {
		t.Errorf("ToReverseString() should be %s, but got %s", dll.ToString(), reveseString)
	}

	for i, n := 0, dll.Size(); i < n; i++ {
		_ = dll.Pop()
		// fmt.Printf("Poped: %d\n", sdk.val)
	}
	if dll.ToString() != "" {
		t.Errorf("ToString() should be empty, but got %s", dll.ToString())
	}
}

func valstr2arr(val_str string) [][]string {
	val_str = val_str[2 : len(val_str)-2]
	val_arr := strings.Split(val_str, "],[")
	rst := make([][]string, len(val_arr))
	for i := 0; i < len(val_arr); i++ {
		vals := strings.Split(val_arr[i], ",")
		rst[i] = vals
	}
	return rst
}
func TestLinkedHashList(t *testing.T) {
	fmt.Println("TestLinkedHashList")
	list := NewLinkedHashList[int]()
	maxSize := 2
	op1 := []string{"LRUCache", "get", "put", "get", "put", "put", "get", "get"}
	val1_str := "[[2],[2],[2,6],[1],[1,5],[1,2],[1],[2]]"
	val1 := valstr2arr(val1_str)
	var rst []byte = []byte("null,")
	for i := 1; i < len(op1); i++ {
		switch op1[i] {
		case "get":
			tmp := list.Get(val1[i][0])
			if tmp != nil {
				rst = append(rst, []byte(fmt.Sprintf("%d", *tmp))...)
			} else {
				rst = append(rst, []byte("-1")...)
			}
			rst = append(rst, byte(','))
		case "put":
			tmp, _ := strconv.Atoi(val1[i][1])
			list.Set(val1[i][0], tmp)
			rst = append(rst, []byte("null,")...)
		}
		if list.Len() > maxSize {
			list.Pop()
		}
		// fmt.Printf("list :%s\n", list.ToString())
	}
	// fmt.Printf("rst :%s\n", string(rst))
	op2 := []string{"LRUCache", "put", "put", "get", "put", "get", "put", "get", "get", "put", "get", "get"}
	val2_str := "[[3],[1,1],[2,2],[1],[3,3],[2],[4,4],[1],[3],[5,5],[1],[3]]"
	val2 := valstr2arr(val2_str)
	rst2 := "null,null,null,1,null,2,null,-1,3,null,-1,3,"

	list = NewLinkedHashList[int]()
	maxSize = 3

	rst = []byte("null,")
	for i := 1; i < len(op2); i++ {
		switch op2[i] {
		case "get":
			tmp := list.Get(val2[i][0])
			if tmp != nil {
				rst = append(rst, []byte(fmt.Sprintf("%d", *tmp))...)
			} else {
				rst = append(rst, []byte("-1")...)
			}
			rst = append(rst, byte(','))
		case "put":
			tmp, _ := strconv.Atoi(val2[i][1])
			list.Set(val2[i][0], tmp)
			rst = append(rst, []byte("null,")...)
		}
		if list.Len() > maxSize {
			list.Pop()
		}
		// fmt.Printf("list :%s\n", list.ToString())
	}
	if string(rst) != rst2 {
		t.Errorf("rst should be %s , but got %s", rst2, string(rst))
	}

}
