package afero

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheOnCreate(t *testing.T) {
	cacheTime := time.Millisecond * 50

	base := NewOsFs()
	layer := NewMemMapFs()
	composite := NewCacheOnCreateFs(base, layer, cacheTime)
	defer composite.Close()

	dir, err := TempDir(composite, "", "cache-on-create-test")
	require.NoError(t, err)
	require.NotEmpty(t, dir)
	defer base.RemoveAll(dir)

	fp := filepath.Join(dir, "test0.txt")
	d := []byte("helloworld")
	// Create file in composite fs
	f := requireFileCreate(t, composite, fp, d)
	// File should exist in all fs
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, layer, fp, d)
	requireFileExist(t, base, fp, d)
	// Allow cache time to elapse
	time.Sleep(cacheTime * 2)
	// File should exist in all fs
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, layer, fp, d)
	requireFileExist(t, base, fp, d)
	// Original file handle should still work
	requireFileRead(t, f, d) // Closes file handle
	// Allow file in layer fs to expire
	time.Sleep(cacheTime * 2)
	// File should exist in composite fs and base fs but not layer fs
	requireFileExist(t, composite, fp, d)
	requireFileNotExist(t, layer, fp)
	requireFileExist(t, base, fp, d)

	fp = filepath.Join(dir, "test1-0.txt")
	fp2 := filepath.Join(dir, "test1-1.txt")
	d = []byte("helloworld")
	// Create file in composite fs
	f = requireFileCreate(t, composite, fp, d)
	if runtime.GOOS == "windows" {
		requireFileRead(t, f, d)
	}
	// Rename file in composite fs
	requireFileRename(t, composite, fp, fp2)
	// File should exist in all fs at new path but not old path
	requireFileNotExist(t, composite, fp)
	requireFileExist(t, composite, fp2, d)
	requireFileNotExist(t, layer, fp)
	requireFileExist(t, layer, fp2, d)
	requireFileNotExist(t, base, fp)
	requireFileExist(t, base, fp2, d)
	// Allow renamed file in layer fs to expire
	time.Sleep(cacheTime * 2)
	// File should exist in composite fs and base fs but not layer fs at new path but not old path
	requireFileNotExist(t, composite, fp)
	requireFileExist(t, composite, fp2, d)
	requireFileNotExist(t, layer, fp)
	requireFileNotExist(t, layer, fp2)
	requireFileNotExist(t, base, fp)
	requireFileExist(t, base, fp2, d)
	// Original file handle should still work
	if runtime.GOOS != "windows" {
		requireFileRead(t, f, d)
	}

	fp = filepath.Join(dir, "test2-orig.txt")
	fp2 = filepath.Join(dir, "test2-link.txt")
	d = []byte("helloworld")
	// Create file in composite fs
	f = requireFileCreate(t, composite, fp, d)
	// Link file in composite fs
	requireFileLink(t, composite, fp, fp2)
	// File should exist in all fs at both old path and new path
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, composite, fp2, d)
	requireFileExist(t, layer, fp, d)
	requireFileExist(t, layer, fp2, d)
	requireFileExist(t, base, fp, d)
	requireFileExist(t, base, fp2, d)
	// Allow linked file in layer fs to expire
	time.Sleep(cacheTime * 2)
	// File should exist in composite fs and base fs but not layer fs at new path but not old path
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, composite, fp2, d)
	requireFileExist(t, layer, fp, d)
	requireFileNotExist(t, layer, fp2)
	requireFileExist(t, base, fp, d)
	requireFileExist(t, base, fp2, d)
	// Original file handle should still work
	requireFileRead(t, f, d) // Closes file handle
	// Allow file in layer fs to expire
	time.Sleep(cacheTime * 2)
	// File should exist in composite fs and base fs but not layer fs at both old path and new path
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, composite, fp2, d)
	requireFileNotExist(t, layer, fp)
	requireFileNotExist(t, layer, fp2)
	requireFileExist(t, base, fp, d)
	requireFileExist(t, base, fp2, d)

	fp = filepath.Join(dir, "test3.txt")
	d = []byte("test_base")
	d2 := []byte("test_layer")
	// File should not exist yet in any fs
	requireFileNotExist(t, composite, fp)
	requireFileNotExist(t, layer, fp)
	requireFileNotExist(t, base, fp)
	// Create file in base fs
	f = requireFileCreate(t, base, fp, d)
	requireFileRead(t, f, d)
	// File should exist in composite fs and base fs but not layer fs
	requireFileExist(t, composite, fp, d)
	requireFileNotExist(t, layer, fp)
	requireFileExist(t, base, fp, d)
	// File in composite fs should point to file in base fs
	requireFileStat(t, composite, fp, d)
	// Reset file
	requireFileRemove(t, composite, fp, nil)
	// Create file in layer fs
	f = requireFileCreate(t, layer, fp, d2)
	requireFileRead(t, f, d2)
	// File should exist in composite fs and layer fs but not base fs
	requireFileExist(t, composite, fp, d2)
	requireFileExist(t, layer, fp, d2)
	requireFileNotExist(t, base, fp)
	// File in composite fs should point to file in layer fs
	requireFileStat(t, composite, fp, d2)
	// Removing file in composite fs should fail (no file in base fs)
	requireFileRemove(t, composite, fp, os.IsNotExist)
	// Reset file
	requireFileRemove(t, layer, fp, nil)
	// Create file in both base fs and layer fs
	f = requireFileCreate(t, base, fp, d)
	requireFileRead(t, f, d)
	f = requireFileCreate(t, layer, fp, d2)
	requireFileRead(t, f, d2)
	// File should exist in all fs
	requireFileExist(t, composite, fp, d2)
	requireFileExist(t, layer, fp, d2)
	requireFileExist(t, base, fp, d)
	// File in composite fs should point to file in layer fs
	requireFileStat(t, composite, fp, d2)
	// Reset file
	requireFileRemove(t, composite, fp, nil)
	// File should not exist in any fs
	requireFileNotExist(t, composite, fp)
	requireFileNotExist(t, layer, fp)
	requireFileNotExist(t, base, fp)

	fp = filepath.Join(dir, "test4.txt")
	d = []byte("test_base")
	d2 = []byte("test_layer")
	d3 := append(d, d2...)
	// File should not exist yet in composite fs
	requireFileOpen(t, composite, fp, os.O_RDONLY, nil, os.IsNotExist)
	requireFileOpen(t, composite, fp, os.O_RDWR, nil, os.IsNotExist)
	// Create file in composite fs
	f = requireFileOpen(t, composite, fp, os.O_CREATE|os.O_RDWR, d, nil)
	requireFileRead(t, f, d)
	// File should exist in all fs
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, layer, fp, d)
	requireFileExist(t, base, fp, d)
	// Allow renamed file in layer fs to expire
	time.Sleep(cacheTime * 2)
	// File should exist in composite fs and base fs but not layer fs
	requireFileExist(t, composite, fp, d)
	requireFileNotExist(t, layer, fp)
	requireFileExist(t, base, fp, d)
	// Create file in base fs
	f = requireFileOpen(t, base, fp, os.O_CREATE|os.O_RDWR, d, nil)
	requireFileRead(t, f, d)
	// File should exist in composite fs
	requireFileExist(t, composite, fp, d)
	// File in composite fs should point to file in base fs
	f = requireFileOpen(t, composite, fp, os.O_RDONLY, nil, nil)
	requireFileRead(t, f, d)
	// Reset file
	requireFileRemove(t, composite, fp, nil)
	// Create file in layer fs
	f = requireFileOpen(t, layer, fp, os.O_CREATE|os.O_RDWR, d2, nil)
	requireFileRead(t, f, d2)
	// File should exist in composite fs
	requireFileExist(t, composite, fp, d2)
	// File in composite fs should point to file in layer fs
	f = requireFileOpen(t, composite, fp, os.O_RDONLY, nil, nil)
	requireFileRead(t, f, d2)
	// Reset file
	requireFileRemove(t, layer, fp, nil)
	// Create file in both base fs and layer fs with different data
	f = requireFileOpen(t, base, fp, os.O_CREATE|os.O_RDWR, d, nil)
	requireFileRead(t, f, d)
	f = requireFileOpen(t, layer, fp, os.O_CREATE|os.O_RDWR, d2, nil)
	requireFileRead(t, f, d2)
	// File should exist in composite fs
	requireFileExist(t, composite, fp, d2)
	// File in composite fs with read flag should point to file in layer fs
	f = requireFileOpen(t, composite, fp, os.O_RDONLY, nil, nil)
	requireFileRead(t, f, d2)
	// Reset file
	requireFileRemove(t, composite, fp, nil)
	// Create file in both base fs and layer fs with same data
	f = requireFileOpen(t, base, fp, os.O_CREATE|os.O_RDWR, d, nil)
	requireFileRead(t, f, d)
	f = requireFileOpen(t, layer, fp, os.O_CREATE|os.O_RDWR, d, nil)
	requireFileRead(t, f, d)
	// File should exist in composite fs
	requireFileExist(t, composite, fp, d)
	// File in composite fs with write flag should point to file in both base fs and layer fs
	f = requireFileOpen(t, composite, fp, os.O_WRONLY, nil, nil)
	requireFileWrite(t, f, d2)
	requireFileRead(t, f, d3)
	requireFileExist(t, composite, fp, d3)
	requireFileExist(t, layer, fp, d3)
	requireFileExist(t, base, fp, d3)
	// Reset file
	requireFileRemove(t, composite, fp, nil)
	// File should not exist in any fs
	requireFileNotExist(t, composite, fp)
	requireFileNotExist(t, layer, fp)
	requireFileNotExist(t, base, fp)

	dp := filepath.Join(dir, "test5")
	dp2 := filepath.Join(dp, "data")
	fp = filepath.Join(dp2, "0.txt")
	fp2 = filepath.Join(dp2, "1.txt")
	d = []byte("test_base")
	d2 = []byte("test_layer")
	// Create first dir in base fs
	requireDirMake(t, base, dp)
	// Create second dir in composite fs; should not fail though first dir did not exist in layer fs
	requireDirMake(t, composite, dp2)
	// Create first file in base fs
	f = requireFileCreate(t, base, fp, d)
	requireFileRead(t, f, d)
	// Create second file in layer fs
	f = requireFileCreate(t, layer, fp2, d2)
	requireFileRead(t, f, d2)
	// Files should exist in composite fs
	requireFileExist(t, composite, fp, d)
	requireFileExist(t, composite, fp2, d2)
	// Dir in composite fs should have both files
	requireDirRead(t, composite, dp2, map[string][]byte{fp: d, fp2: d2})
}

func TestCacheOnCreateFsClose(t *testing.T) {
	composite := NewCacheOnCreateFs(NewOsFs(), NewMemMapFs(), time.Millisecond*50)

	require.NoError(t, composite.Close())
	require.NoError(t, composite.Close()) // Close must be idempotent
}

func requireFileCreate(t *testing.T, fs Fs, fp string, d []byte) File {
	f, err := fs.Create(fp)
	require.NoError(t, err)
	require.NotNil(t, f)
	n, err := f.Write(d)
	require.NoError(t, err)
	require.Equal(t, len(d), n)
	return f
}

func requireFileOpen(t *testing.T, fs Fs, fp string, flag int, d []byte, fn func(error) bool) File {
	f, err := fs.OpenFile(fp, flag, 0o666)
	if fn == nil {
		require.NoError(t, err)
		require.NotNil(t, f)
		if flag&os.O_CREATE > 0 && len(d) > 0 {
			n, err := f.Write(d)
			require.NoError(t, err)
			require.Equal(t, len(d), n)
		}
		return f
	} else {
		require.Error(t, err)
		require.True(t, fn(err))
		return nil
	}
}

func requireFileWrite(t *testing.T, f File, d []byte) {
	_, err := f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	n, err := f.Write(d)
	require.NoError(t, err)
	require.Equal(t, len(d), n)
}

func requireFileRead(t *testing.T, f File, d []byte) {
	n, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	p, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, d, p)
	err = f.Close()
	require.NoError(t, err)
}

func requireFileExist(t *testing.T, fs Fs, fp string, d []byte) {
	f, err := fs.Open(fp)
	require.NoError(t, err)
	require.NotNil(t, f)
	p, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, d, p)
	err = f.Close()
	require.NoError(t, err)
}

func requireFileNotExist(t *testing.T, fs Fs, fp string) {
	_, err := fs.Open(fp)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func requireFileStat(t *testing.T, fs Fs, fp string, d []byte) {
	fi, err := fs.Stat(fp)
	require.NoError(t, err)
	require.NotNil(t, fi)
	require.Equal(t, int64(len(d)), fi.Size())
	f, err := fs.Open(fp)
	require.NoError(t, err)
	require.NotNil(t, f)
	fi, err = f.Stat()
	require.NoError(t, err)
	require.NotNil(t, fi)
	require.Equal(t, int64(len(d)), fi.Size())
	err = f.Close()
	require.NoError(t, err)
}

func requireFileRename(t *testing.T, fs Fs, oldFp string, newFp string) {
	err := fs.Rename(oldFp, newFp)
	require.NoError(t, err)
}

func requireFileLink(t *testing.T, fs Fs, oldFp string, newFp string) {
	err := fs.Link(oldFp, newFp)
	require.NoError(t, err)
}

func requireFileRemove(t *testing.T, fs Fs, fp string, fn func(error) bool) {
	err := fs.Remove(fp)
	if fn == nil {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, fn(err))
	}
}

func requireDirMake(t *testing.T, fs Fs, dp string) {
	err := fs.Mkdir(dp, 0o700)
	require.NoError(t, err)
}

func requireDirRead(t *testing.T, fs Fs, dp string, files map[string][]byte) {
	open := func(name string) (File, error) {
		return fs.Open(name)
	}
	openFile := func(name string) (File, error) {
		return fs.OpenFile(name, os.O_RDONLY, 0o666)
	}
	for _, fn := range []func(string) (File, error){open, openFile} {
		f, err := fn(dp)
		require.NoError(t, err)
		require.NotNil(t, f)
		fis, err := f.Readdir(0)
		require.NoError(t, err)
		require.NotEmpty(t, fis)
		require.Len(t, fis, len(files))
		for _, fi := range fis {
			fp := filepath.Join(dp, fi.Name())
			require.Contains(t, files, fp)
			require.Equal(t, int64(len(files[fp])), fi.Size())
		}
	}
}
