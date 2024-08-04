package disklrucache

import (
	"os"
	"strconv"
)

type EditorWriter struct {
	file   *os.File
	editor *DiskLRUCacheEditor
}

func (w *EditorWriter) Write(p []byte) (n int, err error) {
	n, err = w.file.Write(p)
	w.editor.writeSize += int64(n)
	return n, err
}

func (w *EditorWriter) Close() error {
	return w.file.Close()
}

type AutoRemoveReadCloser struct {
	file *os.File
}

func (r *AutoRemoveReadCloser) Read(p []byte) (n int, err error) {
	return r.file.Read(p)
}

func (r *AutoRemoveReadCloser) Close() error {
	err := r.file.Close()
	if err == nil {
		os.Remove(r.file.Name())
	}
	return err
}

func GetAvailableTmpFilename(name string) string {
	for i := 0; i < 10000; i++ {
		tmpName := name + ".tmp" + strconv.Itoa(i)
		if _, err := os.Stat(tmpName); os.IsNotExist(err) {
			return tmpName
		}
	}
	return ""
}

func RenameFile(oldName, newName string, overwrite bool) error {
	if _, err := os.Stat(newName); !os.IsNotExist(err) {
		if overwrite {
			os.Remove(newName)
		} else {
			return os.ErrExist
		}
	}
	return os.Rename(oldName, newName)
}
