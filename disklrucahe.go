package disklrucache

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_FILE_SIZE        = ^uint32(0)
	JOURNAL_FILENAME     = "journal"
	JOURNAL_TMP_FILENAME = "journal.tmp"
	JOURNAL_BACKUP_FILE  = "journal.bak"
	DIRTY                = "dirty"
	CLEAN                = "clean"
	DEL                  = "del"
	READ                 = "read"
	FILE_HEAD            = "go-disklrucache"
)

type CacheEntry struct {
	base      *DiskLRUCache
	key       string
	size      int64 //file size
	readable  bool
	commitId  uint32
	curEditor *DiskLRUCacheEditor
	time      time.Time
}

func (entry *CacheEntry) GetDirtyFilename() string {
	return getAvailableTmpFilename(entry.GetCleanFilename())
}

func (entry *CacheEntry) GetCleanFilename() string {
	return path.Join(entry.base.cachePath, entry.key)
}

type DiskLRUCache struct {
	entries       LinkedHashList[CacheEntry]
	lock          sync.RWMutex
	cacheVersion  int
	appVersion    int
	sequential_id uint32 //begin from 1
	cachePath     string
	maxSize       int64
	curSize       int64
	journalFile   *os.File
}

type DiskLRUCacheEditor struct {
	base        *DiskLRUCache
	entry       *CacheEntry
	lock        sync.RWMutex
	isError     bool
	commited    bool
	writeSize   int64
	tmpFilename string
}

// get the size that have written,do not care overlap
func (editor *DiskLRUCacheEditor) WriteSize() int64 {
	return editor.writeSize
}

// get the true filesize
func (editor *DiskLRUCacheEditor) FileSize() int64 {
	info, err := os.Stat(editor.tmpFilename)
	if err != nil {
		return 0
	}
	return info.Size()
}
func (editor *DiskLRUCacheEditor) maxSize() int64 {
	return editor.entry.base.maxSize
}

func (cache *DiskLRUCache) checkNotClosed() {
	if cache.journalFile == nil {
		panic("cache journal file is closed")
	}
}

// need lock manually
func (cache *DiskLRUCache) checkFull() {
	for cache.curSize > cache.maxSize {
		entry := cache.entries.Pop()
		if entry.curEditor != nil {
			log.Println("warning: a uncommited entry is popped,may be cache size is too small")
			entry.curEditor = nil
		}
		os.Remove(entry.GetCleanFilename())
		if entry.size == 0 {
			log.Panicf("fatal error, entry size is 0, key:%s", entry.key)
		}
		cache.curSize -= entry.size
		cache.journalFile.WriteString(
			fmt.Sprintf("%s %s\n", DEL, entry.key),
		)
	}
}

func (cache *DiskLRUCache) Edit(name string) *DiskLRUCacheEditor {
	cache.checkNotClosed()
	cache.lock.Lock()
	defer cache.lock.Unlock()
	entry := cache.entries.Get(name)
	// insert new entry if not exist
	if entry == nil {
		node := cache.entries.Set(name, CacheEntry{
			base:      cache,
			key:       name,
			size:      0,
			readable:  false,
			commitId:  0,
			curEditor: nil,
		})
		entry = &node.val
	}
	//do not change readable status for that snapshot should not stuck by write
	if entry.curEditor != nil {
		return nil
	}
	editor := &DiskLRUCacheEditor{base: cache, entry: entry, lock: sync.RWMutex{}, isError: false, commited: false, writeSize: 0, tmpFilename: ""}
	entry.curEditor = editor
	entry.time = time.Now()
	cache.journalFile.WriteString(
		fmt.Sprintf("%s %s\n", DIRTY, name),
	)
	return editor
}

// Will remove anyway, even if editor is not commited
func (cache *DiskLRUCache) Remove(name string) {
	cache.checkNotClosed()
	cache.lock.Lock()
	defer cache.lock.Unlock()
	entry := cache.entries.Del(name)
	if entry == nil {
		return
	}
	entry.curEditor = nil
	//only remove clean file, dirty file will be removed when commit
	os.Remove(entry.GetCleanFilename())
	cache.curSize -= entry.size
	cache.journalFile.WriteString(
		fmt.Sprintf("%s %s\n", DEL, name),
	)
}

// will return a output stream, which will record write num
func (editor *DiskLRUCacheEditor) CreateOutputStream() (io.WriteCloser, error) {
	editor.lock.Lock()
	editor.tmpFilename = editor.entry.GetDirtyFilename()
	file, err := os.OpenFile(editor.tmpFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		editor.isError = true
	}
	return &EditorWriter{file: file, editor: editor}, err
}

func (editor *DiskLRUCacheEditor) CreateAppendStream() (io.WriteCloser, error) {
	editor.lock.Lock()
	if editor.tmpFilename == "" {
		editor.tmpFilename = editor.entry.GetDirtyFilename()
	}
	file, err := os.OpenFile(editor.tmpFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		editor.isError = true
	}
	return &EditorWriter{file: file, editor: editor}, err
}
func (editor *DiskLRUCacheEditor) CreateRandomWriter() (*EditorWriter, error) {
	editor.lock.Lock()
	editor.tmpFilename = editor.entry.GetDirtyFilename()
	file, err := os.OpenFile(editor.tmpFilename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		editor.isError = true
	}
	return &EditorWriter{file: file, editor: editor}, err
}

// Would Not lock
func (editor *DiskLRUCacheEditor) CreateInputStream() (io.ReadCloser, error) {
	if editor.entry.readable == false {
		return nil, nil
	}
	if runtime.GOOS == "windows" {
		tmpName := getAvailableLinkname(editor.entry.GetCleanFilename())
		err := os.Link(editor.entry.GetCleanFilename(), tmpName)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(tmpName, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		return &AutoRemoveReader{File: file}, nil
	}
	file, err := os.OpenFile(editor.entry.key, os.O_RDONLY, 0666)
	return file, err
}

func (editor *DiskLRUCacheEditor) Commit() error {
	defer editor.lock.Unlock()
	editor.base.lock.Lock()
	defer editor.base.lock.Unlock()

	if editor.entry.curEditor != editor {
		//remove before commit
		os.Remove(editor.tmpFilename)
		return nil
	}
	if editor.isError {
		//TODO: deal error
		os.Remove(editor.tmpFilename)
		os.Remove(editor.entry.GetCleanFilename())
		return nil
	}

	editor.entry.curEditor = nil
	editor.base.curSize += (editor.FileSize() - editor.entry.size)
	editor.entry.size = editor.FileSize()
	editor.commited = true
	editor.entry.readable = true
	editor.entry.commitId = editor.base.sequential_id
	editor.base.sequential_id++

	_, err := editor.base.journalFile.WriteString(
		fmt.Sprintf("%s %s %d %d\n", CLEAN, editor.entry.key,
			editor.entry.size, editor.entry.time.UnixMilli()),
	)
	if err != nil {
		os.Remove(editor.tmpFilename)
		return err
	}
	renameFile(editor.tmpFilename, editor.entry.GetCleanFilename(), true)

	editor.base.checkFull()
	return err
}

type DiskLRUCacheSnapshot struct {
	Key    string
	Size   int64
	Reader Reader
	Time   time.Time
}

func (cache *DiskLRUCache) Get(key string) (*DiskLRUCacheSnapshot, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	cache.checkNotClosed()
	entry := cache.entries.Get(key)
	if entry == nil {
		return nil, nil
	}
	//wait data ready
	// if entry.readable == false && entry.curEditor != nil {
	// 	curEditor := entry.curEditor
	// 	cache.lock.RUnlock()
	// 	entry.curEditor.lock.RLock()
	// 	cache.lock.RLock()
	// 	// wait fail
	// 	if entry.readable == false {
	// 		if entry.curEditor != curEditor {
	// 			entry.curEditor.lock.RUnlock()
	// 		}
	// 		return nil, nil
	// 	}
	// }
	var reader Reader
	// for windows,open a link to avoid file lock
	if runtime.GOOS == "windows" {
		tmpName := getAvailableLinkname(entry.GetCleanFilename())
		err := os.Link(entry.GetCleanFilename(), tmpName)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(tmpName, os.O_RDONLY, 0666)
		if err != nil {
			if err == os.ErrNotExist {
				log.Printf("warning: cache %s exist,but file not exist", key)
			}
			return nil, err
		}
		reader = &AutoRemoveReader{File: file}

	} else {
		file, err := os.OpenFile(entry.GetCleanFilename(), os.O_RDONLY, 0666)
		if err != nil {
			if err == os.ErrNotExist {
				log.Printf("warning: cache %s exist,but file not exist", key)
			}
			return nil, err
		}
		reader = file
	}

	cache.journalFile.WriteString(
		fmt.Sprintf("%s %s\n", READ, key),
	)
	return &DiskLRUCacheSnapshot{key, entry.size, reader, entry.time}, nil
}

func CreateDiskLRUCache(cachePath string, appVersion int, cacheVersion int, maxsize int64) *DiskLRUCache {

	cache := &DiskLRUCache{
		entries:       *NewLinkedHashList[CacheEntry](),
		lock:          sync.RWMutex{},
		appVersion:    appVersion,
		cacheVersion:  cacheVersion,
		sequential_id: 1,
		cachePath:     cachePath,
		maxSize:       maxsize,
		curSize:       0,
		journalFile:   nil,
	}
	if err := cache.init(); err != nil {
		log.Panicf("init lru cache failed,err:%s", err)
	}
	return cache
}
func (cache *DiskLRUCache) init() error {
	if _, err := os.Stat(cache.cachePath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(cache.cachePath, 0777); err != nil {
			return err
		}
	}
	// has journal file
	if _, err := os.Stat(filepath.Join(cache.cachePath, JOURNAL_FILENAME)); err == nil {
		file, err := os.OpenFile(filepath.Join(cache.cachePath, JOURNAL_FILENAME), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		cache.journalFile = file
		cache.parseFile(file)
		return nil
	}
	//no journal file, create a new one
	f, err := cache.newJournal(JOURNAL_FILENAME)
	if err == nil {
		cache.journalFile = f
	}
	return err
}

// use to cretae new journal file
func (cache *DiskLRUCache) newJournal(filename string) (*os.File, error) {
	if _, err := os.Stat(path.Join(cache.cachePath, filename)); err != nil && os.IsExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(cache.cachePath, filename), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	//write meta data
	f.WriteString(fmt.Sprintf("%s\n", FILE_HEAD))
	f.WriteString(fmt.Sprintf("%d %d %d\n", cache.appVersion, cache.cacheVersion, cache.maxSize))
	return f, nil

}
func (cache *DiskLRUCache) RebuildJournal() error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	file, err := cache.newJournal(JOURNAL_TMP_FILENAME)
	if err != nil {
		panic("Create or truncate journal file failed")
	}
	iterator := cache.entries.Iterator()
	for iterator.Next() {
		entry := iterator.Value()
		if entry.curEditor != nil || entry.readable == false {
			file.WriteString(fmt.Sprintf("%s %s\n", DIRTY, entry.key))
		} else {
			file.WriteString(fmt.Sprintf("%s %s %d %d\n", CLEAN, entry.key, entry.size, entry.time.UnixMilli()))
		}
	}
	file.Close()
	if cache.journalFile != nil {
		cache.journalFile.Close()
		cache.journalFile = nil
	}
	// backup old journal file and rename to new
	err = renameFile(filepath.Join(cache.cachePath, JOURNAL_FILENAME), filepath.Join(cache.cachePath, JOURNAL_BACKUP_FILE), true)
	if err != nil {
		log.Printf("warning: rename journal file failed,err:%s", err)
	}
	err = renameFile(filepath.Join(cache.cachePath, JOURNAL_TMP_FILENAME), filepath.Join(cache.cachePath, JOURNAL_FILENAME), true)
	if err != nil {
		log.Panicf("rename new journal file failed,err:%s", err)
	}
	file, _ = os.OpenFile(filepath.Join(cache.cachePath, JOURNAL_FILENAME), os.O_RDWR, 0666)
	cache.journalFile = file
	return nil
}

func (cache *DiskLRUCache) parseFile(file io.Reader) error {
	need_rebuild := false
	scanner := bufio.NewReader(file)
	line, isPrefix, err := scanner.ReadLine()
	if err != nil || isPrefix {
		return NewJournalFileFormatError()
	}
	if string(line) != "go-disklrucache" {
		return NewJournalFileFormatError()
	}
	line, isPrefix, _ = scanner.ReadLine()
	strs := strings.Split(strings.TrimSpace(string(line)), " ")
	if len(strs) != 3 {
		return NewJournalFileFormatError()
	}
	appVersion, err1 := strconv.Atoi(strs[0])
	cacheVersion, err2 := strconv.Atoi(strs[1])
	maxSize, err2 := strconv.ParseInt(strs[2], 10, 64)
	if err1 != nil || err2 != nil {
		return NewJournalFileFormatError()
	}
	if diff := appVersion - cache.appVersion + (cacheVersion - cache.cacheVersion); diff != 0 {
		return NewJournalVersionError()
	}
	if cache.maxSize != 0 && maxSize != cache.maxSize {
		log.Printf("Warning: max size in journal file is %d, but current max size is %d,rebuild\n", maxSize, MAX_FILE_SIZE)
		need_rebuild = true
	}
	cache.cacheVersion = cacheVersion
	cache.appVersion = appVersion
	dirtyMap := make(map[string]*DoublyLinkedListNode[CacheEntry])
	for {
		line, isPrefix, err = scanner.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if isPrefix {
			return NewJournalFileFormatError()
		}
		strs = strings.Split(strings.TrimSpace(string(line)), " ")
		operator := strs[0]
		if operator == DIRTY {
			dirty_node := cache.entries.Set(strs[1], CacheEntry{
				base:      cache,
				key:       strs[1],
				size:      0,
				readable:  false,
				commitId:  0,
				curEditor: nil,
			})
			dirtyMap[strs[1]] = dirty_node
		} else if operator == CLEAN {
			node, ok := dirtyMap[strs[1]]
			var entry *CacheEntry
			if !ok {
				//the rebuiild journal will not have dirty entry
				node := cache.entries.Set(strs[1], CacheEntry{
					base:      cache,
					key:       strs[1],
					size:      0,
					readable:  true,
					commitId:  0,
					curEditor: nil,
				})
				entry = &node.val
			} else {
				//change dirty entry to clean entry won't change the order of LRU
				//so we will get these nodes from dirtyMap
				entry = &node.val
			}
			entry.commitId = cache.sequential_id
			cache.sequential_id += 1
			entry.size, _ = strconv.ParseInt(strs[2], 10, 64)
			entry.readable = true
			timeStamp, _ := strconv.ParseInt(strs[3], 10, 64)
			entry.time = time.UnixMilli(timeStamp)
		} else if operator == READ {
			cache.entries.Get(strs[1])
		} else if operator == DEL {
			entry := cache.entries.Get(strs[1])
			if entry != nil {
				cache.entries.Del(strs[1])
			}
		} else {
			log.Panicf("unknown journal operator:%s", operator)
		}
	}
	if need_rebuild {
		if err := cache.RebuildJournal(); err != nil {
			return err
		}
		//if cache size become larger than max size, we need shrink the cache
		cache.lock.Lock()
		defer cache.lock.Unlock()
		cache.checkFull()
	}
	return nil
}

func (cache *DiskLRUCache) Close() error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	if cache.journalFile != nil {
		cache.journalFile.Close()
	}
	cache.journalFile = nil
	return nil
}
