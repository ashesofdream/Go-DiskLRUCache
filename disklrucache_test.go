package disklrucache

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const (
	CACHE_DIR      = "./test/cache"
	DATA_DIR       = "./test/data"
	letterBytes    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	TEST_DATA_NUM  = 100
	TEST_DATA_SIZE = 1024 * 1024
	// ClEAR_TEST_DIR = true
)

func CreateRandomFile(dir string, size int) (string, error) {

	filename := make([]byte, 10)
	for i := range filename {
		filename[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	file, _ := os.Create(filepath.Join(dir, string(filename)))
	defer file.Close()
	data := make([]byte, size)
	for i := range data {
		data[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	_, err := file.Write(data)
	if err != nil {
		return "", err
	}
	return string(filename), nil
}

func CreateRandomFiles(dir string, num int, totalSize int) error {
	for i := 0; i < num; i++ {

		_, err := CreateRandomFile(dir, rand.Intn(totalSize/num))
		if err != nil {
			return err
		}
	}
	return nil
}

type fileData struct {
	filename string
	size     int64
	data     []byte
}

func GetAllTestData() []fileData {
	files := make([]fileData, 0)
	err := filepath.Walk(DATA_DIR, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		data := make([]byte, info.Size())
		_, err = file.Read(data)
		if err != nil {
			return err
		}
		files = append(files, fileData{
			filename: info.Name(),
			size:     info.Size(),
			data:     data,
		})
		return nil
	})
	if err != nil {
		panic(err)
	}
	return files
}

func TestGenData(t *testing.T) {
	fmt.Printf("Generating test data...\n")
	os.RemoveAll(CACHE_DIR)
	os.RemoveAll(DATA_DIR)
	if _, err := os.Stat(CACHE_DIR); os.IsNotExist(err) {
		os.MkdirAll(CACHE_DIR, 0755)
	}
	if _, err := os.Stat(DATA_DIR); os.IsNotExist(err) {
		os.MkdirAll(DATA_DIR, 0755)
	}
	err := CreateRandomFiles(DATA_DIR, TEST_DATA_NUM, TEST_DATA_SIZE)
	if err != nil {
		t.Error(err)
	}
}

func TestLRUCache(t *testing.T) {
	fmt.Printf("Testing LRUCache...\n")
	data := GetAllTestData()

	for _, d := range data {
		if d.size > 1024*128 {
			t.Errorf("data size too large, %d", d.size)
		}

	}
	err := os.RemoveAll(CACHE_DIR)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		rand.Shuffle(len(data), func(i, j int) {
			data[i], data[j] = data[j], data[i]
		})
		cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
		idx := 0
		data_num := len(data)
		totalSize := int64(0)
		for ; idx < data_num; idx++ {
			d := data[idx]
			editor := cache.Edit(d.filename)
			if editor == nil {
				t.Error("Edit failed")
				return
			}
			writer, err := editor.CreateOutputStream()
			if err != nil {
				t.Error(err)
				return
			}
			writer.Write(d.data)
			writer.Close()
			editor.Commit()
			totalSize += d.size
			if totalSize > cache.maxSize {
				// pop
				break
			}
			if cache.curSize != totalSize {
				t.Fatalf("curSize error, %d %d", cache.curSize, totalSize)
			}
		}
		// push data to pop all
		totalSize = 0
		firstIdx := idx + 1
		idx = firstIdx
		cacheSizeArr := []int64{}
		for ; idx < data_num; idx++ {
			d := data[idx]
			editor := cache.Edit(d.filename)
			if editor == nil {
				t.Error("Edit failed")
				return
			}
			writer, err := editor.CreateOutputStream()
			if err != nil {
				t.Error(err)
				return
			}
			writer.Write(d.data)
			writer.Close()
			editor.Commit()
			totalSize += d.size
			cacheSizeArr = append(cacheSizeArr, int64(d.size))
			if totalSize > cache.maxSize {
				// pop
				idx := 0
				for totalSize > cache.maxSize {
					totalSize -= cacheSizeArr[idx]
					idx += 1
				}
				cacheSizeArr = cacheSizeArr[idx:]
				break
			}
		}
		//get pop data should be nil
		secondIdx := idx + 1

		for i := 0; i < firstIdx; i++ {
			d := data[i]
			cache_data, err := cache.Get(d.filename)
			if err != nil {
				t.Error(err)
				return
			}
			if cache_data != nil {
				t.Error("poll data should be nil")
				return
			}
		}
		totalSize = 0
		for i := firstIdx; i < secondIdx; i++ {
			d := data[i]
			cache_data, err := cache.Get(d.filename)
			if err != nil {
				t.Error(err)
				return
			}
			if cache_data == nil && totalSize == 0 {
				continue
			}
			if cache_data == nil {
				t.Error("cache data should not be nil")
			}
			totalSize += cache_data.Size
			cur_data := make([]byte, cache_data.Size)
			cache_data.Reader.Read(cur_data)
			if bytes.Compare(cur_data, data[i].data) != 0 {
				t.Errorf("data not equal, %s\n,%s\n", string(cur_data), string(data[i].data))
			}
		}
		sum := int64(0)
		for _, cache_data := range cacheSizeArr {
			sum += cache_data
		}
		if totalSize != sum {
			t.Errorf("totalSize error, %d %d", totalSize, sum)
		}
		cache.Close()
		if thres := float32(totalSize) / float32(cache.maxSize); thres < 0.9 {
			t.Errorf("cache rst size should be more than 90%% of total data size,but only %f", thres)
		}
		os.RemoveAll(CACHE_DIR)
	}

}

func InsertDataRoutine(cache *DiskLRUCache, data []fileData, productSignal chan int, isRunning *bool, t *testing.T) {
	for *isRunning {
		idx := rand.Intn(len(data))
		d := data[idx]
		editor := cache.Edit(d.filename)
		if editor == nil {
			continue
		}
		writer, err := editor.CreateOutputStream()
		if err != nil {
			t.Errorf("InsertDataRoutine: %s", err)
			return
		}
		n, err := writer.Write(d.data)
		if editor.WriteSize() != int64(n) {
			t.Errorf("InsertDataRoutine: curSize error, %d,true %d", editor.WriteSize(), int64(n))
			return
		}
		writer.Close()
		editor.Commit()

		productSignal <- idx
	}
}

func ReadDataRoutine(cache *DiskLRUCache, data []fileData, productSignalchan chan int, isRunning *bool, will_miss_cache bool, t *testing.T) {
	for *isRunning {
		// idx := rand.Intn(len(data))
		idx := <-productSignalchan
		d := data[idx]
		snapshot, err := cache.Get(d.filename)
		if err != nil {
			continue
		}
		if snapshot == nil {
			if !will_miss_cache {
				t.Error("set will_miss_cache to true,but cache miss")
			}
			continue
		}
		cur_data := make([]byte, snapshot.Size)
		n, err := snapshot.Reader.Read(cur_data)
		f, _ := snapshot.Reader.(*AutoRemoveReader)
		var name = f.Name()
		if n == 0 {
			t.Errorf("Name:%s", name)
			t.Error("ReadDataRoutine: read data size error")
		}
		if snapshot.Size != int64(len(d.data)) {
			t.Errorf("cache data size error, %d %d", snapshot.Size, int64(len(d.data)))
			*isRunning = false
			return
		}
		if rst := bytes.Compare(cur_data, d.data); rst != 0 {
			t.Errorf("data not equal, rst:%d", rst)
		}
		snapshot.Reader.Close()
	}
}
func TestLRUCacheRacing(t *testing.T) {
	fmt.Printf("Testing LRUCache Racing...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)

	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	isRunning := true
	productSignal := make(chan int, TEST_DATA_NUM/10/2)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, true, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, true, t)
	time.Sleep(5 * time.Second)
	isRunning = false
	time.Sleep(1 * time.Second)
	cache.Close()
}

func TestLRUCacheRacing2(t *testing.T) {
	//TODO: size not correct
	fmt.Printf("Testing LRUCache Racing2...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)

	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE)
	isRunning := true
	productSignal := make(chan int, TEST_DATA_NUM/10/2)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, false, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, false, t)
	time.Sleep(3 * time.Second)
	isRunning = false
	time.Sleep(1 * time.Second)
	cache.Close()

}

func TestLRUCacheRebuildJournal(t *testing.T) {
	fmt.Printf("Testing LRUCache RebuildJournal...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)

	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
	for i := 0; i < len(data); i++ {
		d := data[i]
		editor := cache.Edit(d.filename)
		if editor == nil {
			t.Error("Edit failed")
			return
		}
		writer, err := editor.CreateOutputStream()
		if err != nil {
			t.Error(err)
			return
		}
		writer.Write(d.data)
		writer.Close()
		editor.Commit()
	}
	cache.Close()
	fmt.Println("Test Origin Journal First")
	origin_cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	target_list := cache.entries.data_list
	target_map := cache.entries.data_map
	origin_list := origin_cache.entries.data_list
	if target_list.size != origin_list.size {
		t.Errorf("origin_list len error, origin:%d, target:%d", target_list.size, origin_list.size)
		return
	}
	target_cur := target_list.head
	origin_cur := origin_list.head
	for target_cur != nil && origin_cur != nil {
		if target_cur.val.key != origin_cur.val.key ||
			target_cur.val.size != origin_cur.val.size {
			t.Errorf("origin_list data error, origin:%s, target:%s,orgin_size:%d,target_size:%d",
				origin_cur.val.key, target_cur.val.key, origin_cur.val.size, target_cur.val.size)
			return
		}
		target_cur = target_cur.next
		origin_cur = origin_cur.next
	}
	fmt.Println("Test Origin Journal Success")

	fmt.Println("Test Rebuild Journal")
	origin_cache.RebuildJournal()
	origin_cache.Close()

	rebuid_cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	rebuild_list := rebuid_cache.entries.data_list
	if rebuild_list.size != target_list.size {
		t.Errorf("rebuild_list len error, rebuild:%d, target:%d", rebuild_list.size, target_list.size)
		return
	}
	rebuild_cur := rebuild_list.head
	target_cur = target_list.head
	for rebuild_cur != nil && target_cur != nil {
		if rebuild_cur.val.key != target_cur.val.key ||
			rebuild_cur.val.size != target_cur.val.size {
			t.Errorf("rebuild_list data error, rebuild:%s, target:%s", rebuild_cur.val.key, target_cur.val.key)
			return
		}
		rebuild_cur = rebuild_cur.next
		target_cur = target_cur.next
	}
	rebuild_map := rebuid_cache.entries.data_map
	if len(rebuild_map) != len(target_map) {
		t.Errorf("rebuild_map len error, rebuild:%d, target:%d", len(rebuild_map), len(target_map))
		return
	}
	for key, node := range rebuild_map {
		if node.val.size != target_map[key].val.size {
			t.Errorf("rebuild_map data size error, rebuild:%d, target:%d", node.val.size, target_map[key].val.size)
			return
		}
	}
	rebuid_cache.Close()

	fmt.Println("Test Rebuild Journal Success")

}

func TestLRUCacheRacingRebuild(t *testing.T) {
	fmt.Printf("Create Racing data...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)

	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	isRunning := true
	productSignal := make(chan int, TEST_DATA_NUM/10/2)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go InsertDataRoutine(cache, data, productSignal, &isRunning, t)
	go DelDataRoutine(cache, data, productSignal, &isRunning, true, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, true, t)
	go ReadDataRoutine(cache, data, productSignal, &isRunning, true, t)
	go DelDataRoutine(cache, data, productSignal, &isRunning, true, t)
	time.Sleep(3 * time.Second)
	isRunning = false
	time.Sleep(1 * time.Second)
	cache.Close()

	fmt.Println("Test Racing Rebuild Journal Origin")
	orgin_cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	orgin_list := orgin_cache.entries.data_list
	if orgin_list.size == 0 {
		t.Errorf("racing data size is 0,please check test data")
		return
	}
	orgin_head := orgin_list.head
	target_head := cache.entries.data_list.head
	cnt := 0
	for orgin_head != nil && target_head != nil {
		if orgin_head.val.key != target_head.val.key ||
			orgin_head.val.size != target_head.val.size ||
			orgin_head.val.time.UnixMilli() != target_head.val.time.UnixMilli() {
			t.Errorf("node:%d,orgin_list data error, orgin:%s, target:%s,orgin_size:%d,target_size:%d,"+
				"orgin_time:%d,target_time:%d", cnt,
				orgin_head.val.key, target_head.val.key,
				orgin_head.val.size, target_head.val.size,
				orgin_head.val.time.UnixMilli(), target_head.val.time.UnixMilli(),
			)
			cnt++
			return
		}
		orgin_head = orgin_head.next
		target_head = target_head.next
	}
	orgin_cache.Close()

	fmt.Println("Test Racing Rebuild Journal")
	cache.RebuildJournal()
	target_list := cache.entries.data_list
	target_map := cache.entries.data_map
	if target_list.size == 0 {
		t.Errorf("racing data size is 0,please check test data")
		return
	}
	rebuid_cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	rebuild_list := rebuid_cache.entries.data_list
	rebuild_map := rebuid_cache.entries.data_map
	if rebuild_list.size != target_list.size {
		t.Errorf("rebuild_list len error, rebuild:%d, target:%d", rebuild_list.size, target_list.size)
		return
	}
	rebuild_cur := rebuild_list.head
	target_cur := target_list.head
	for rebuild_cur != nil && target_cur != nil {
		if rebuild_cur.val.key != target_cur.val.key ||
			rebuild_cur.val.size != target_cur.val.size {
			t.Errorf("rebuild_list data error, rebuild:%s, target:%s", rebuild_cur.val.key, target_cur.val.key)
			return
		}
		rebuild_cur = rebuild_cur.next
		target_cur = target_cur.next
	}
	if len(rebuild_map) != len(target_map) {
		t.Errorf("rebuild_map len error, rebuild:%d, target:%d", len(rebuild_map), len(target_map))
		return
	}
	for key, node := range rebuild_map {
		if node.val.size != target_map[key].val.size {
			t.Errorf("rebuild_map data size error, rebuild:%d, target:%d", node.val.size, target_map[key].val.size)
			return
		}
	}
	rebuid_cache.Close()
	fmt.Println("Test Racing Rebuild Journal Success")

}

func TestSnapShot(t *testing.T) {
	fmt.Printf("Testing SnapShot...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	key_div := len(data)
	for i := 0; i < len(data); i++ {
		key := data[i/key_div].filename
		val := data[i].data
		editor := cache.Edit(key)
		if editor == nil {
			t.Error("Edit failed")
			return
		}
		if i%key_div != 0 {
			snapshot, err := cache.Get(key)
			if err != nil {
				t.Error(err)
				return
			}
			// write data now
			writer, err := editor.CreateOutputStream()
			if err != nil {
				t.Error(err)
				return
			}

			writer.Write(val)
			writer.Close()
			editor.Commit()
			snap_data := make([]byte, snapshot.Size)
			snapshot.Reader.Read(snap_data)
			snapshot.Reader.Close()
			if bytes.Compare(snap_data, data[i-1].data) != 0 {
				t.Errorf("snap_data not equal, snap_size:%d,true_size:%d", len(snap_data), len(data[i-1].data))
			}
		} else {
			writer, err := editor.CreateOutputStream()
			if err != nil {
				t.Error(err)
				return
			}
			writer.Write(val)
			writer.Close()
			editor.Commit()
		}
	}
}

func TestRemoveReader(t *testing.T) {
	fmt.Printf("Testing RemoveReader...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)
	if runtime.GOOS == "windows" {
		cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
		key := data[0].filename
		editor := cache.Edit(key)
		strem, err := editor.CreateOutputStream()
		cache_path := editor.entry.GetCleanFilename() + ".link0"
		if err != nil {
			t.Error(err)
			return
		}
		strem.Write(data[0].data)
		strem.Close()
		editor.Commit()

		snapshot, err := cache.Get(key)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := os.Stat(cache_path); os.IsNotExist(err) {
			t.Error("ReadLink Not Exist")
			return
		}
		snapshot.Reader.Close()
		if _, err := os.Stat(cache_path); err == nil {
			t.Error("ReadLink Should be deleted")
			return
		}

		cache.Close()

	}
}

func TestRemove(t *testing.T) {
	fmt.Printf("Testing Remove...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)
	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	key := data[0].filename
	val := data[0].data
	//Test Remove After Edit
	editor := cache.Edit(key)
	writer, _ := editor.CreateOutputStream()
	writer.Write(val)
	writer.Close()
	editor.Commit()
	cache.Remove(key)
	if cache.curSize != 0 {
		t.Errorf("curSize should be 0, but %d", cache.curSize)
	}
	if _, err := os.Stat(editor.entry.GetCleanFilename()); os.IsExist(err) {
		t.Errorf("clean file shoud be deleted")
		return
	}
	// Test Remove When editing
	editor = cache.Edit(key)
	cache.Remove(key)
	writer, _ = editor.CreateOutputStream()
	writer.Write(val)
	editor.Commit()
	writer.Close()
	if cache.curSize != 0 {
		t.Errorf("curSize should be 0, but %d", cache.curSize)
	}
	if _, err := os.Stat(editor.entry.GetCleanFilename()); os.IsExist(err) {
		t.Errorf("clean file shoud be deleted")
	}
	if _, err := os.Stat(editor.tmpFilename); os.IsExist(err) {
		t.Errorf("dirty file shoud be deleted")
	}

	// Test Remove When Write
	editor = cache.Edit(key)
	writer, _ = editor.CreateOutputStream()
	cache.Remove(key)
	writer.Write(val)
	writer.Close()
	editor.Commit()
	if cache.curSize != 0 {
		t.Errorf("curSize should be 0, but %d", cache.curSize)
	}
	if _, err := os.Stat(editor.tmpFilename); os.IsExist(err) {
		t.Errorf("dirty file shoud be deleted")
	}
	if _, err := os.Stat(editor.entry.GetCleanFilename()); os.IsExist(err) {
		t.Errorf("clean file shoud be deleted")
	}
	cache.Close()
	return
}

func DelDataRoutine(cache *DiskLRUCache, data []fileData, productSignalchan chan int, isRunning *bool, will_miss_cache bool, t *testing.T) {
	for *isRunning || len(productSignalchan) > 0 {
		// idx := rand.Intn(len(data))
		idx := <-productSignalchan
		d := data[idx]
		cache.Remove(d.filename)
	}
}
func InsertDataRoutine2(cache *DiskLRUCache, data []fileData, productSignalchan chan int, isRunning *bool, will_miss_cache bool, t *testing.T) {
	for *isRunning {
		idx := rand.Intn(len(data))
		d := data[idx]
		editor := cache.Edit(d.filename)
		if editor == nil {
			continue
		}
		productSignalchan <- idx
		writer, err := editor.CreateOutputStream()
		if err != nil {
			t.Error(err)
			return
		}
		writer.Write(d.data)
		writer.Close()
		editor.Commit()
	}
}

func TestRacingDel(t *testing.T) {
	fmt.Printf("Testing Racing Del...\n")
	data := GetAllTestData()
	os.RemoveAll(CACHE_DIR)
	cache := CreateDiskLRUCache(CACHE_DIR, 1, 1, TEST_DATA_SIZE/10)
	isRunning := true
	productSignal := make(chan int, TEST_DATA_NUM/10/10)
	routine_num := 3
	for i := 0; i < routine_num; i++ {
		go InsertDataRoutine2(cache, data, productSignal, &isRunning, false, t)
		go DelDataRoutine(cache, data, productSignal, &isRunning, false, t)
	}
	time.Sleep(3 * time.Second)
	isRunning = false
	time.Sleep(1 * time.Second)
	cache.Close()
	if cache.curSize != 0 {
		t.Errorf("curSize should be 0, but %d", cache.curSize)
	}
	return

}
