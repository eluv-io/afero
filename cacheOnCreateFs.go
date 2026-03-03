package afero

import (
	"os"
	"path/filepath"
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
// minimum, for the cache time after the creation of the file. Renaming/moving
// a file will reset the cache time for the file at the new path.
//
// This caching union will forward all write calls also to the base filesystem
// after the layer filesystem. To prevent writing to the base Fs, wrap it in a
// read-only filter.
//
// Note: This simplified implementation has the following caveat(s):
//   - For Unix-like filesystems, open file handles should continue to function
//     without issue, even after the file expires. For other filesystems, the
//     behavior of open file handles after file expiration is not specified.
//   - If a file is created, removed, and re-created within cacheTime, the file
//     may expire pre-maturely, in which case it will be removed from the layer
//     filesystem but still accessible via the base filesystem. See note above
//     for behavior of open file handles for expired files.
type CacheOnCreateFs struct {
	base       Fs
	layer      Fs
	cacheTime  time.Duration
	cacheFiles chan *cacheFile
}

type cacheFile struct {
	name       string
	expiration time.Time
}

func NewCacheOnCreateFs(base Fs, layer Fs, cacheTime time.Duration) Fs {
	u := &CacheOnCreateFs{base: base, layer: layer, cacheTime: cacheTime}
	// Needs to be sufficiently large enough for expected concurrent unexpired cached files
	u.cacheFiles = make(chan *cacheFile, 8192)
	go func() {
		// Remove expired files in cacheFiles
		// Since cacheTime is fixed, files in cacheFiles will expire in order and so can be processed in order
		for file := range u.cacheFiles {
			now := time.Now()
			if file.expiration.After(now) {
				// Not expired yet; wait for expiration
				time.Sleep(file.expiration.Sub(now))
			}
			// Expired
			err := u.layer.Remove(file.name)
			if err != nil && !isNotExist(err) { // Ignore file if already removed (or renamed)
				// Log error and retry by re-adding to cacheFiles
				log.Warn("afero.CacheOnWriteFs: failed to remove cached file",
					err, "file", file.name)
				u.cacheFiles <- file
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
	lf, err := u.layer.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		_ = bf.Close()
		return nil, err
	}
	u.cacheFile(name)
	return &UnionFile{Base: bf, Layer: lf}, nil
}

func (u *CacheOnCreateFs) Open(name string) (f File, err error) {
	defer func() { err = sanitize(err) }()

	bf, err := u.base.Open(name)
	if err != nil && !isNotExist(err) {
		return nil, err
	}
	lf, err := u.layer.Open(name)
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
	lf, err := u.layer.OpenFile(name, flag, perm)
	if err != nil && !isNotExist(err) {
		return nil, err
	}

	if flag&os.O_CREATE > 0 {
		u.cacheFile(name)
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
		u.cacheFile(newname)
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
		u.cacheFile(newname)
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

func (u *CacheOnCreateFs) cacheFile(name string) {
	if u.cacheTime > 0 {
		u.cacheFiles <- &cacheFile{name: filepath.Clean(name), expiration: time.Now().Add(u.cacheTime)}
	}
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
