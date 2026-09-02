// Copyright © 2015 Steve Francia <spf@spf13.com>.
// Copyright 2013 tsuru authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mem

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/afero/internal/common"
)

const FilePathSeparator = string(filepath.Separator)

var _ fs.ReadDirFile = &File{}

type File struct {
	// atomic requires 64-bit alignment for struct field access
	at           int64
	readDirCount int64
	dirBuf       []*FileData
	dirBufOnce   sync.Once
	closed       bool
	readOnly     bool
	fileData     *FileData
}

func NewFileHandle(data *FileData) *File {
	return &File{fileData: data}
}

func NewReadOnlyFileHandle(data *FileData) *File {
	return &File{fileData: data, readOnly: true}
}

func (f *File) Data() *FileData {
	return f.fileData
}

type FileData struct {
	sync.RWMutex
	name    string
	data    *fileBytes
	memDir  Dir
	dir     bool
	mode    os.FileMode
	modtime time.Time
	uid     int
	gid     int
}

func (d *FileData) duplicate() *FileData {
	return &FileData{
		name:    d.name,
		data:    d.data,
		memDir:  d.memDir,
		dir:     d.dir,
		mode:    d.mode,
		modtime: d.modtime,
		uid:     d.uid,
		gid:     d.gid,
	}
}

func (d *FileData) Name() string {
	d.RLock()
	defer d.RUnlock()
	return d.name
}

func CreateFile(name string) *FileData {
	return &FileData{
		name:    name,
		data:    &fileBytes{},
		mode:    os.ModeTemporary,
		modtime: time.Now(),
	}
}

func CreateDir(name string) *FileData {
	return &FileData{
		name:    name,
		data:    &fileBytes{},
		memDir:  &DirMap{},
		dir:     true,
		modtime: time.Now(),
	}
}

func CreateLink(f *FileData, newname string) *FileData {
	f.Lock()
	f2 := f.duplicate()
	f2.name = newname
	if f2.data.m == nil { // m may be non-nil if creating a link of a link
		f2.data.m = &sync.RWMutex{}
	}
	f.Unlock()
	return f2
}

func ChangeFileName(f *FileData, newname string) {
	f.Lock()
	f.name = newname
	f.Unlock()
}

func SetMode(f *FileData, mode os.FileMode) {
	f.Lock()
	f.mode = mode
	f.Unlock()
}

func SetModTime(f *FileData, mtime time.Time) {
	f.Lock()
	setModTime(f, mtime)
	f.Unlock()
}

func setModTime(f *FileData, mtime time.Time) {
	f.modtime = mtime
}

func SetUID(f *FileData, uid int) {
	f.Lock()
	f.uid = uid
	f.Unlock()
}

func SetGID(f *FileData, gid int) {
	f.Lock()
	f.gid = gid
	f.Unlock()
}

func GetFileInfo(f *FileData) *FileInfo {
	return &FileInfo{f}
}

func (f *File) Open() error {
	atomic.StoreInt64(&f.at, 0)
	atomic.StoreInt64(&f.readDirCount, 0)
	f.fileData.Lock()
	f.closed = false
	f.fileData.Unlock()
	return nil
}

func (f *File) Close() error {
	f.fileData.Lock()
	f.closed = true
	if !f.readOnly {
		setModTime(f.fileData, time.Now())
	}
	f.fileData.Unlock()
	return nil
}

func (f *File) Name() string {
	return f.fileData.Name()
}

func (f *File) Stat() (os.FileInfo, error) {
	return &FileInfo{f.fileData}, nil
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Readdir(count int) (res []os.FileInfo, err error) {
	f.fileData.RLock()
	defer f.fileData.RUnlock()
	if f.closed {
		return nil, ErrFileClosed
	}
	if !f.fileData.dir {
		return nil, &os.PathError{
			Op:   "readdir",
			Path: f.fileData.name,
			Err:  errors.New("not a dir"),
		}
	}
	var outLength int64

	f.dirBufOnce.Do(func() {
		f.dirBuf = f.fileData.memDir.Files()
	})
	cur := atomic.LoadInt64(&f.readDirCount)
	files := f.dirBuf[cur:]
	if count > 0 {
		if len(files) < count {
			outLength = int64(len(files))
		} else {
			outLength = int64(count)
		}
		if len(files) == 0 {
			err = io.EOF
		}
	} else {
		outLength = int64(len(files))
	}
	atomic.StoreInt64(&f.readDirCount, cur+outLength)

	res = make([]os.FileInfo, outLength)
	for i := range res {
		res[i] = &FileInfo{files[i]}
	}

	return res, err
}

func (f *File) Readdirnames(n int) (names []string, err error) {
	fi, err := f.Readdir(n)
	names = make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = filepath.Split(f.Name())
	}
	return names, err
}

// Implements fs.ReadDirFile
func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	fi, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	di := make([]fs.DirEntry, len(fi))
	for i, f := range fi {
		di[i] = common.FileInfoDirEntry{FileInfo: f}
	}
	return di, nil
}

func (f *File) Read(b []byte) (n int, err error) {
	f.fileData.RLock()
	defer f.fileData.RUnlock()
	if f.closed {
		return 0, ErrFileClosed
	}
	cur := atomic.LoadInt64(&f.at)
	f.fileData.data.RLock()
	defer f.fileData.data.RUnlock()
	if len(b) > 0 && int(cur) == len(f.fileData.data.d) {
		return 0, io.EOF
	}
	if int(cur) > len(f.fileData.data.d) {
		return 0, io.ErrUnexpectedEOF
	}
	if len(f.fileData.data.d)-int(cur) >= len(b) {
		n = len(b)
	} else {
		n = len(f.fileData.data.d) - int(cur)
	}
	copy(b, f.fileData.data.d[cur:cur+int64(n)])
	atomic.StoreInt64(&f.at, cur+int64(n))
	return
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	prev := atomic.LoadInt64(&f.at)
	atomic.StoreInt64(&f.at, off)
	n, err = f.Read(b)
	atomic.StoreInt64(&f.at, prev)
	return
}

func (f *File) Truncate(size int64) error {
	f.fileData.Lock()
	defer f.fileData.Unlock()
	if f.closed {
		return ErrFileClosed
	}
	if f.readOnly {
		return &os.PathError{
			Op:   "truncate",
			Path: f.fileData.name,
			Err:  errors.New("file handle is read only"),
		}
	}
	if size < 0 {
		return ErrOutOfRange
	}
	f.fileData.data.Lock()
	defer f.fileData.data.Unlock()
	if size > int64(len(f.fileData.data.d)) {
		diff := size - int64(len(f.fileData.data.d))
		f.fileData.data.d = append(f.fileData.data.d, bytes.Repeat([]byte{0o0}, int(diff))...)
	} else {
		f.fileData.data.d = f.fileData.data.d[0:size]
	}
	setModTime(f.fileData, time.Now())
	return nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.fileData.RLock()
	defer f.fileData.RUnlock()
	if f.closed {
		return 0, ErrFileClosed
	}
	cur := offset
	switch whence {
	case io.SeekStart:
		atomic.StoreInt64(&f.at, cur)
	case io.SeekCurrent:
		cur = atomic.AddInt64(&f.at, cur)
	case io.SeekEnd:
		f.fileData.data.RLock()
		cur = int64(len(f.fileData.data.d)) + offset
		atomic.StoreInt64(&f.at, cur)
		f.fileData.data.RUnlock()
	}
	return cur, nil
}

func (f *File) Write(b []byte) (n int, err error) {
	f.fileData.Lock()
	defer f.fileData.Unlock()
	if f.closed {
		return 0, ErrFileClosed
	}
	if f.readOnly {
		return 0, &os.PathError{
			Op:   "write",
			Path: f.fileData.name,
			Err:  errors.New("file handle is read only"),
		}
	}
	n = len(b)
	cur := atomic.LoadInt64(&f.at)
	f.fileData.data.Lock()
	defer f.fileData.data.Unlock()
	diff := cur - int64(len(f.fileData.data.d))
	var tail []byte
	if n+int(cur) < len(f.fileData.data.d) {
		tail = f.fileData.data.d[n+int(cur):]
	}
	if diff > 0 {
		f.fileData.data.d = append(
			f.fileData.data.d,
			append(bytes.Repeat([]byte{0o0}, int(diff)), b...)...)
		f.fileData.data.d = append(f.fileData.data.d, tail...)
	} else {
		f.fileData.data.d = append(f.fileData.data.d[:cur], b...)
		f.fileData.data.d = append(f.fileData.data.d, tail...)
	}
	setModTime(f.fileData, time.Now())

	atomic.StoreInt64(&f.at, cur+int64(n))
	return
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	atomic.StoreInt64(&f.at, off)
	return f.Write(b)
}

func (f *File) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *File) Info() *FileInfo {
	return &FileInfo{f.fileData}
}

type FileInfo struct {
	*FileData
}

// Implements os.FileInfo
func (s *FileInfo) Name() string {
	s.RLock()
	_, name := filepath.Split(s.name)
	s.RUnlock()
	return name
}

func (s *FileInfo) Mode() os.FileMode {
	s.RLock()
	defer s.RUnlock()
	return s.mode
}

func (s *FileInfo) ModTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.modtime
}

func (s *FileInfo) IsDir() bool {
	s.RLock()
	defer s.RUnlock()
	return s.dir
}
func (s *FileInfo) Sys() interface{} { return nil }
func (s *FileInfo) Size() int64 {
	s.RLock()
	defer s.RUnlock()
	if s.dir {
		return int64(42)
	}
	s.data.RLock()
	defer s.data.RUnlock()
	return int64(len(s.data.d))
}

type fileBytes struct {
	d []byte
	m *sync.RWMutex
}

func (b *fileBytes) Lock() {
	if b.m != nil {
		b.m.Lock()
	}
}

func (b *fileBytes) RLock() {
	if b.m != nil {
		b.m.RLock()
	}
}

func (b *fileBytes) RUnlock() {
	if b.m != nil {
		b.m.RUnlock()
	}
}

func (b *fileBytes) Unlock() {
	if b.m != nil {
		b.m.Unlock()
	}
}

var (
	ErrFileClosed        = errors.New("File is closed")
	ErrOutOfRange        = errors.New("out of range")
	ErrTooLarge          = errors.New("too large")
	ErrFileNotFound      = os.ErrNotExist
	ErrFileExists        = os.ErrExist
	ErrDestinationExists = os.ErrExist
)
