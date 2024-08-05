package disklrucache

import (
	"io"
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

func (w *EditorWriter) Seek(offset int64, whence int) (int64, error) {
	return w.file.Seek(offset, whence)
}

func (w *EditorWriter) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = w.file.WriteAt(p, off)
	w.editor.writeSize += int64(n)
	return n, err
}

type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}
type AutoRemoveReader struct {
	*os.File
}

func (r *AutoRemoveReader) Close() error {
	err := r.File.Close()
	if err == nil {
		os.Remove(r.Name())
	}
	return err
}
func getAvailableTmpFilename(name string) string {
	for i := 0; i < 10000; i++ {
		tmpName := name + ".tmp" + strconv.Itoa(i)
		if _, err := os.Stat(tmpName); os.IsNotExist(err) {
			return tmpName
		}
	}
	return ""
}
func getAvailableLinkname(name string) string {
	for i := 0; i < 10000; i++ {
		linkName := name + ".link" + strconv.Itoa(i)
		if _, err := os.Stat(linkName); os.IsNotExist(err) {
			return linkName
		}
	}
	return ""
}

func renameFile(oldName, newName string, overwrite bool) error {
	if _, err := os.Stat(newName); !os.IsNotExist(err) {
		if overwrite {
			os.Remove(newName)
		} else {
			return os.ErrExist
		}
	}
	return os.Rename(oldName, newName)
}
