package fwi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/pkg/client"
)

/*
	NOTE: This is an idea that is partially fleshed out - it is not considered "ready for use"
*/

// FS represents a virtual file system interface.
type FS interface {
	// Open opens the named file for reading.
	Open(ctx context.Context, path string) (File, error)
	// Create creates or truncates the named file.
	Create(ctx context.Context, path string) (File, error)
	// Remove removes the named file or directory.
	// By default, it will not remove non-empty directories.
	// Use WithRecursiveRemove to delete a directory and all its contents.
	Remove(ctx context.Context, path string, opts ...RemoveOption) error
	// Mkdir creates a new directory.
	Mkdir(ctx context.Context, path string) error
	// ReadDir reads the directory named by dirname and returns a list of
	// directory entries sorted by filename.
	ReadDir(ctx context.Context, path string) ([]FileInfo, error)
	// Stat returns a FileInfo describing the named file.
	Stat(ctx context.Context, path string) (FileInfo, error)
}

// File represents a file in the virtual file system.
type File interface {
	io.ReadWriteSeeker
	io.Closer
	Stat() (FileInfo, error)
}

// FileInfo provides metadata about a file.
type FileInfo interface {
	Name() string
	Size() int64
	IsDir() bool
	ModTime() time.Time
}

// RemoveOption is a functional option for the Remove method.
type RemoveOption func(*removeOptions)

type removeOptions struct {
	Recursive bool
}

// WithRecursiveRemove enables recursive deletion of directories.
func WithRecursiveRemove() RemoveOption {
	return func(o *removeOptions) {
		o.Recursive = true
	}
}

const (
	vfsMetaPrefix    = "vfs:meta:"
	vfsBlockPrefix   = "vfs:block:"
	defaultBlockSize = 8192
)

// NewVFS creates a new FS instance.
func NewVFS(vs KV, logger *slog.Logger) FS {
	return &vfsImpl{
		vs:     vs,
		logger: logger.WithGroup("vfs"),
	}
}

// vfsImpl implements the FS interface.
type vfsImpl struct {
	vs     KV
	logger *slog.Logger
}

type fileInfoImpl struct {
	FileName    string    `json:"name"`
	FileSize    int64     `json:"size"`
	FileModTime time.Time `json:"mod_time"`
	FIsDir      bool      `json:"is_dir"`
	BlockSize   int       `json:"block_size"`
	BlockCount  int       `json:"block_count"`
	path        string
}

func (fi *fileInfoImpl) Name() string       { return fi.FileName }
func (fi *fileInfoImpl) Size() int64        { return fi.FileSize }
func (fi *fileInfoImpl) IsDir() bool        { return fi.FIsDir }
func (fi *fileInfoImpl) ModTime() time.Time { return fi.FileModTime }

// fileImpl implements the File interface.
type fileImpl struct {
	vfs     *vfsImpl
	info    *fileInfoImpl
	reader  io.ReadSeeker
	buffer  *bytes.Buffer
	dirty   bool
	isWrite bool
}

var _ FS = &vfsImpl{}
var _ File = &fileImpl{}
var _ FileInfo = &fileInfoImpl{}

func (v *vfsImpl) Open(ctx context.Context, path string) (File, error) {
	info, err := v.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("cannot open directory: %s", path)
	}

	fileInfo := info.(*fileInfoImpl)
	var data []byte

	if fileInfo.BlockCount > 0 {
		data = make([]byte, 0, fileInfo.FileSize)
		for i := 0; i < fileInfo.BlockCount; i++ {
			blockKey := fmt.Sprintf("%s%s:%d", vfsBlockPrefix, path, i)
			blockData, err := v.vs.Get(ctx, blockKey)
			if err != nil {
				if errors.Is(err, client.ErrKeyNotFound) {
					return nil, fmt.Errorf("missing block %d for file %s", i, path)
				}
				return nil, fmt.Errorf("failed to get block %d: %w", i, err)
			}
			data = append(data, []byte(blockData)...)
		}
	}

	return &fileImpl{
		vfs:     v,
		info:    fileInfo,
		reader:  bytes.NewReader(data),
		isWrite: false,
	}, nil
}

func (v *vfsImpl) Create(ctx context.Context, path string) (File, error) {
	// Check if parent directory exists.
	parent := filepath.Dir(path)
	if parent != "." && parent != "/" {
		parentInfo, err := v.Stat(ctx, parent)
		if err != nil {
			return nil, fmt.Errorf("parent directory does not exist: %w", err)
		}
		if !parentInfo.IsDir() {
			return nil, fmt.Errorf("parent is not a directory: %s", parent)
		}
	}

	info := &fileInfoImpl{
		FileName:    filepath.Base(path),
		FileSize:    0,
		FileModTime: time.Now(),
		FIsDir:      false,
		BlockSize:   defaultBlockSize,
		BlockCount:  0,
		path:        path,
	}

	if err := v.setMeta(ctx, path, info); err != nil {
		return nil, fmt.Errorf("failed to set meta for new file: %w", err)
	}

	return &fileImpl{
		vfs:     v,
		info:    info,
		buffer:  new(bytes.Buffer),
		isWrite: true,
	}, nil
}

func (v *vfsImpl) Remove(ctx context.Context, p string, opts ...RemoveOption) error {
	options := &removeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	info, err := v.getMeta(ctx, p)
	if err != nil {
		// Make remove idempotent. If it's already gone, that's success.
		if errors.Is(err, client.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("failed to stat path for remove: %w", err)
	}

	if info.IsDir() {
		// Check if directory is empty
		children, err := v.ReadDir(ctx, p)
		if err != nil {
			return fmt.Errorf("failed to read dir for remove: %w", err)
		}
		if len(children) > 0 {
			if !options.Recursive {
				return fmt.Errorf("directory not empty: %s", p)
			}

			// If recursive, delete children first.
			for _, child := range children {
				childPath := path.Join(p, child.Name())
				// Pass the options down to the recursive call.
				if err := v.Remove(ctx, childPath, opts...); err != nil {
					return err // Return the error from the child deletion.
				}
			}
		}
	} else {
		for i := 0; i < info.BlockCount; i++ {
			blockKey := fmt.Sprintf("%s%s:%d", vfsBlockPrefix, info.path, i)
			if err := v.vs.Delete(ctx, blockKey); err != nil {
				if !errors.Is(err, client.ErrKeyNotFound) {
					v.logger.Warn("failed to delete block, proceeding", "path", p, "block", i, "error", err)
				}
			}
		}
	}

	// Remove the metadata entry for the file or the now-empty directory.
	return v.vs.Delete(ctx, vfsMetaPrefix+info.path)
}

func (v *vfsImpl) Mkdir(ctx context.Context, path string) error {
	// Check if parent directory exists.
	parent := filepath.Dir(path)
	if parent != "." && parent != "/" {
		parentInfo, err := v.Stat(ctx, parent)
		if err != nil {
			return fmt.Errorf("parent directory does not exist: %w", err)
		}
		if !parentInfo.IsDir() {
			return fmt.Errorf("parent is not a directory: %s", parent)
		}
	}

	info := &fileInfoImpl{
		FileName:    filepath.Base(path),
		FileModTime: time.Now(),
		FIsDir:      true,
		path:        path,
	}

	// Check if something already exists at this path
	if _, err := v.getMeta(ctx, path); err == nil {
		return fmt.Errorf("file or directory already exists: %s", path)
	}

	return v.setMeta(ctx, path, info)
}

func (v *vfsImpl) ReadDir(ctx context.Context, path string) ([]FileInfo, error) {
	info, err := v.getMeta(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", path)
	}

	// Ensure path has a trailing slash for prefix iteration
	prefix := path
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	keys, err := v.vs.IterateKeys(ctx, vfsMetaPrefix+prefix, 0, 1024)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate keys for readdir: %w", err)
	}

	var infos []FileInfo
	for _, key := range keys {
		// We only want direct children, not grandchildren.
		childPath := strings.TrimPrefix(key, vfsMetaPrefix)
		relativePath := strings.TrimPrefix(childPath, prefix)
		if strings.Contains(relativePath, "/") {
			continue // It's a grandchild, skip.
		}

		childInfo, err := v.getMeta(ctx, childPath)
		if err != nil {
			v.logger.Warn("failed to get meta for child, skipping", "path", childPath, "error", err)
			continue
		}
		infos = append(infos, childInfo)
	}
	return infos, nil
}

func (v *vfsImpl) Stat(ctx context.Context, path string) (FileInfo, error) {
	return v.getMeta(ctx, path)
}

func (v *vfsImpl) getMeta(ctx context.Context, path string) (*fileInfoImpl, error) {
	metaKey := vfsMetaPrefix + path
	val, err := v.vs.Get(ctx, metaKey)
	if err != nil {
		return nil, err
	}

	var info fileInfoImpl
	if err := json.Unmarshal([]byte(val), &info); err != nil {
		return nil, err
	}
	info.path = path
	return &info, nil
}

func (v *vfsImpl) setMeta(ctx context.Context, path string, info *fileInfoImpl) error {
	metaKey := vfsMetaPrefix + path
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return v.vs.Set(ctx, metaKey, string(data))
}

func (f *fileImpl) Read(p []byte) (n int, err error) {
	if f.isWrite {
		return 0, io.EOF // Or an error, reading a write-only file
	}
	return f.reader.Read(p)
}

func (f *fileImpl) Write(p []byte) (n int, err error) {
	if !f.isWrite {
		return 0, errors.New("file not opened for writing")
	}
	f.dirty = true
	return f.buffer.Write(p)
}

func (f *fileImpl) Seek(offset int64, whence int) (int64, error) {
	if f.isWrite {
		return 0, errors.New("cannot seek a file opened for writing")
	}
	return f.reader.Seek(offset, whence)
}

func (f *fileImpl) Close() error {
	if !f.isWrite || !f.dirty {
		return nil
	}

	ctx := context.Background()
	data := f.buffer.Bytes()
	totalSize := len(data)

	blockSize := f.info.BlockSize
	if blockSize == 0 {
		blockSize = defaultBlockSize
	}

	oldBlockCount := f.info.BlockCount
	newBlockCount := (totalSize + blockSize - 1) / blockSize
	if newBlockCount == 0 && totalSize == 0 {
		newBlockCount = 0
	}

	for i := 0; i < newBlockCount; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > totalSize {
			end = totalSize
		}
		blockData := data[start:end]
		blockKey := fmt.Sprintf("%s%s:%d", vfsBlockPrefix, f.info.path, i)
		if err := f.vfs.vs.Set(ctx, blockKey, string(blockData)); err != nil {
			return fmt.Errorf("failed to save block %d: %w", i, err)
		}
	}

	for i := newBlockCount; i < oldBlockCount; i++ {
		blockKey := fmt.Sprintf("%s%s:%d", vfsBlockPrefix, f.info.path, i)
		if err := f.vfs.vs.Delete(ctx, blockKey); err != nil {
			if !errors.Is(err, client.ErrKeyNotFound) {
				f.vfs.logger.Warn("failed to delete old block", "path", f.info.path, "block", i, "error", err)
			}
		}
	}

	f.info.FileSize = int64(totalSize)
	f.info.FileModTime = time.Now()
	f.info.BlockCount = newBlockCount

	if err := f.vfs.setMeta(ctx, f.info.path, f.info); err != nil {
		return fmt.Errorf("failed to update meta on close: %w", err)
	}

	f.dirty = false
	return nil
}

func (f *fileImpl) Stat() (FileInfo, error) {
	return f.info, nil
}
