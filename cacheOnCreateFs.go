package afero

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/eluv-io/log-go"
)

// CacheOnCreateFs is a composite filesystem that wraps a base filesystem with
// a layer filesystem that caches files at file creation for a configured cache
// time. Newly-created, unexpired files are accessed via the layer filesystem
// while also written to the base filesystem. All other files are accessed via
// the base filesystem.
//
// If the cache duration is 0, cache time will be unlimited, i.e. once a file
// is in the layer filesystem, the base filesystem will never be read again for
// this file.
//
// For cache times greater than 0, the layer filesystem will retain a file, at
// minimum, for the cache time after the creation of the file. If a file handle
// remains open when the cache time elapses, the file is not yet removed from
// the layer filesystem and the cache time resets.
//
// This caching union will forward all write calls also to the base filesystem
// after the layer filesystem. To prevent writing to the base Fs, wrap it in a
// read-only filter.
type CacheOnCreateFs struct {
	base  Fs
	layer Fs
	files *cacheFiles
	refs  *cacheRefs
	ttl   time.Duration
}

func NewCacheOnCreateFs(base Fs, layer Fs, cacheTime time.Duration) Fs {
	u := &CacheOnCreateFs{
		base:  base,
		layer: layer,
		files: &cacheFiles{ttl: cacheTime},
		refs:  &cacheRefs{},
		ttl:   cacheTime,
	}
	go func() {
		// Remove expired files in cache file list
		// Since cache time is fixed, cache files in the list will expire in order and so can be processed in order
		for {
			cfile := u.files.Next()
			if cfile == nil {
				// Cache file list is empty; can sleep for cache time and re-check
				time.Sleep(u.ttl)
			} else {
				now := time.Now()
				if cfile.expiration.After(now) {
					// Cache file not expired yet; wait for expiration
					time.Sleep(cfile.expiration.Sub(now))
				}
				// Cache file expired
				var retry bool
				count, release := u.refs.Check(cfile.name)
				if count > 0 {
					// Layer file handle still open; retry later
					retry = true
				} else {
					err := u.layer.Remove(cfile.name)
					if err != nil && !isNotExist(err) { // Ignore file if already removed (or renamed)
						// Log error and retry later
						log.Warn("afero.CacheOnWriteFs: failed to remove cached file",
							err, "file", cfile.name)
						retry = true
					}
				}
				release()
				if retry {
					u.files.Add(cfile.name)
				}
			}
		}
	}()
	return u
}

func (u *CacheOnCreateFs) Name() string {
	return "CacheOnCreateFs"
}

func (u *CacheOnCreateFs) Create(name string) (f File, err error) {
	defer func() { err = sanitize(err) }()
	bf, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	decrement := u.refs.Increment(name)
	lf, err := u.layer.Create(name)
	if err != nil {
		// oops, should we remove? then we have to remember if the file did not exist before
		_ = bf.Close()
		return nil, err
	}
	lf = &onCloseFile{File: lf, onClose: decrement}
	u.files.Add(name)
	return &UnionFile{Base: bf, Layer: lf}, nil
}

func (u *CacheOnCreateFs) Open(name string) (f File, err error) {
	defer func() { err = sanitize(err) }()

	bf, err := u.base.Open(name)
	if err != nil && !isNotExist(err) {
		return nil, err
	}
	decrement := u.refs.Increment(name)
	lf, err := u.layer.Open(name)
	if lf != nil {
		lf = &onCloseFile{File: lf, onClose: decrement}
	} else {
		decrement()
	}
	if err != nil && !isNotExist(err) {
		return nil, err
	}

	if bf == nil && lf == nil {
		// Does not exist in both base and layer; return error
		return nil, os.ErrNotExist
	} else if bf != nil && lf == nil {
		// Only exists in base; use base
		return bf, nil
	} else if bf == nil && lf != nil {
		// Only exists in layer; use layer
		return lf, nil
	}

	fi, err := lf.Stat()
	if err != nil {
		_ = lf.Close()
		_ = bf.Close()
		return nil, err
	} else if !fi.IsDir() {
		// Exists in both base and layer and is not directory; use layer
		_ = bf.Close()
		return lf, nil
	} else {
		// Exists in both base and layer and is directory; use union
		return &UnionFile{Base: bf, Layer: lf}, nil
	}
}

func (u *CacheOnCreateFs) OpenFile(name string, flag int, perm os.FileMode) (f File, err error) {
	const wrFlags = os.O_WRONLY | syscall.O_RDWR | os.O_APPEND | os.O_CREATE | os.O_TRUNC
	defer func() { err = sanitize(err) }()

	bf, err := u.base.OpenFile(name, flag, perm)
	if err != nil && !isNotExist(err) {
		return nil, err
	}
	decrement := u.refs.Increment(name)
	lf, err := u.layer.OpenFile(name, flag, perm)
	if lf != nil {
		lf = &onCloseFile{File: lf, onClose: decrement}
	} else {
		decrement()
	}
	if err != nil && !isNotExist(err) {
		return nil, err
	}

	if flag&os.O_CREATE > 0 {
		u.files.Add(name)
	}

	if bf == nil && lf == nil {
		// Does not exist in both base and layer; return error
		return nil, os.ErrNotExist
	} else if bf != nil && lf == nil {
		// Only exists in base; use base
		return bf, nil
	} else if bf == nil && lf != nil {
		// Only exists in layer; use layer
		return lf, nil
	} else if flag&wrFlags > 0 {
		// Exists in both base and layer and is not read only; use union
		return &UnionFile{Base: bf, Layer: lf}, nil
	}

	fi, err := lf.Stat()
	if err != nil {
		_ = lf.Close()
		_ = bf.Close()
		return nil, err
	} else if !fi.IsDir() {
		// Exists in both base and layer, is read only, and is not directory; use layer
		_ = bf.Close()
		return lf, nil
	} else {
		// Exists in both base and layer, is read only, and is directory; use union
		return &UnionFile{Base: bf, Layer: lf}, nil
	}
}

func (u *CacheOnCreateFs) Stat(name string) (fi os.FileInfo, err error) {
	defer func() { err = sanitize(err) }()
	if fi, err = u.layer.Stat(name); !isNotExist(err) {
		return fi, err
	}
	return u.base.Stat(name)
}

func (u *CacheOnCreateFs) Rename(oldname, newname string) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Rename(oldname, newname); err != nil {
		return err
	}
	err = u.layer.Rename(oldname, newname)
	if err == nil {
		u.files.Add(newname)
	}
	return sanitize(err, true)
}

func (u *CacheOnCreateFs) Link(oldname, newname string) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Link(oldname, newname); err != nil {
		return err
	}
	err = u.layer.Link(oldname, newname)
	if err == nil {
		u.files.Add(newname)
	}
	return sanitize(err, true)
}

func (u *CacheOnCreateFs) Chmod(name string, mode os.FileMode) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Chmod(name, mode); err != nil {
		return err
	}
	return sanitize(u.layer.Chmod(name, mode), true)
}

func (u *CacheOnCreateFs) Chown(name string, uid, gid int) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Chown(name, uid, gid); err != nil {
		return err
	}
	return sanitize(u.layer.Chown(name, uid, gid), true)
}

func (u *CacheOnCreateFs) Chtimes(name string, atime, mtime time.Time) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Chtimes(name, atime, mtime); err != nil {
		return err
	}
	return sanitize(u.layer.Chtimes(name, atime, mtime), true)
}

func (u *CacheOnCreateFs) Remove(name string) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Remove(name); err != nil {
		return err
	}
	return sanitize(u.layer.Remove(name), true)
}

func (u *CacheOnCreateFs) RemoveAll(name string) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.RemoveAll(name); err != nil {
		return err
	}
	return sanitize(u.layer.RemoveAll(name), true)
}

func (u *CacheOnCreateFs) Mkdir(name string, perm os.FileMode) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.Mkdir(name, perm); err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm) // yes, MkdirAll... we cannot assume it exists in the cache
}

func (u *CacheOnCreateFs) MkdirAll(name string, perm os.FileMode) (err error) {
	defer func() { err = sanitize(err) }()
	if err = u.base.MkdirAll(name, perm); err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm)
}

// cacheFiles is a linked list of cache files ordered by time of creation (and time of expiration).
type cacheFiles struct {
	head  *cacheFile
	tail  *cacheFile
	ttl   time.Duration
	mutex sync.Mutex
}

type cacheFile struct {
	name       string
	expiration time.Time
	next       *cacheFile
}

// Add pushes the given cache file to the back of the cache file list.
func (c *cacheFiles) Add(name string) {
	if c.ttl > 0 {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		cfile := &cacheFile{name: filepath.Clean(name), expiration: time.Now().Add(c.ttl)}
		if c.tail == nil {
			c.head = cfile
		} else {
			c.tail.next = cfile
		}
		c.tail = cfile
	}
}

// Next pops the next cache file from the front of the cache file list.
func (c *cacheFiles) Next() *cacheFile {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var cfile *cacheFile
	if c.head != nil {
		cfile = c.head
		c.head = cfile.next
		if c.head == nil {
			c.tail = nil
		}
	}
	return cfile
}

// cacheRefs is a set of ref counts and locks for cache files used to prevent cache files from expiring from the cache
// layer while file handles remain open. When opening a new file handle, Increment should be called prior to the file
// open; the returned Decrement function should be called after the file handle is closed. To check whether a cache file
// can be removed once the cache time has elapsed, Check should be called to return the current ref count; the returned
// Unlock function should be called after any file remove occurs to prevent races with any concurrent file opens.
type cacheRefs struct {
	refs  map[string]*cacheRef
	mutex sync.Mutex
}

type cacheRef struct {
	name  string
	count int
	mutex sync.Mutex
}

// Increment increments the ref count of the given file and exercises the lock of the given file to prevent races.
// Returns a Decrement function that decrements the ref count.
func (c *cacheRefs) Increment(name string) func() {
	cref, decr := c.incr(name)
	cref.mutex.Lock()
	defer cref.mutex.Unlock()
	return decr
}

// Check checks the ref count of the given file and acquires the lock of the given file to prevent races.
// Returns the ref count and an Unlock function that releases the lock.
func (c *cacheRefs) Check(name string) (int, func()) {
	cref, decr := c.incr(name)
	cref.mutex.Lock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	unlock := func() {
		decr()
		cref.mutex.Unlock()
	}
	// Return one less than the current ref count, since incr call above increased ref count by one
	return cref.count - 1, unlock
}

func (c *cacheRefs) incr(name string) (*cacheRef, func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.refs == nil {
		c.refs = make(map[string]*cacheRef)
	}
	name = filepath.Clean(name)
	cref, found := c.refs[name]
	if found {
		cref.count++
	} else {
		cref = &cacheRef{name: name, count: 1}
		c.refs[name] = cref
	}
	decr := func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if cref.count > 0 {
			cref.count--
			return
		}
		delete(c.refs, cref.name)
	}
	return cref, decr
}

type onCloseFile struct {
	File
	onClose func()
}

func (f *onCloseFile) Close() error {
	err := f.File.Close()
	if f.onClose != nil {
		f.onClose()
		f.onClose = nil
	}
	return err
}

func sanitize(err error, ignoreNotExist ...bool) error {
	if len(ignoreNotExist) > 0 && ignoreNotExist[0] && isNotExist(err) {
		return nil
	} else if err == syscall.ENOENT {
		return os.ErrNotExist
	}
	return err
}

func isNotExist(err error) bool {
	return err == syscall.ENOENT || os.IsNotExist(err)
}
