// Copyright (C) 2024 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// Package s3fs implements the fs.Filesystem interface on top of an S3-compatible
// object store (e.g. MinIO, AWS S3).  POSIX metadata (permissions, ownership,
// timestamps) is preserved as custom S3 object metadata with the prefix
// "X-Syncthing-".
package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// Credential / TLS settings can come from the URI query string or from the
// process environment. Query parameters win when present.
//
// Environment variables (first non-empty wins for each field):
//
//	Access key:  S3_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID, MINIO_ACCESS_KEY, MINIO_ROOT_USER
//	Secret key:  S3_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ROOT_PASSWORD
//	Session:     S3_SESSION_TOKEN, AWS_SESSION_TOKEN  (optional)
//	Use SSL:     S3_USE_SSL=true|1  (only if useSSL is not set in the URI)

const FilesystemTypeS3 fs.FilesystemType = "s3"

// S3 object metadata key prefix used to store POSIX attributes.
const metaPrefix = "X-Syncthing-"

// Individual metadata keys (stored under metaPrefix).
const (
	metaKeyMode  = metaPrefix + "Mode"
	metaKeyUID   = metaPrefix + "Uid"
	metaKeyGID   = metaPrefix + "Gid"
	metaKeyAtime = metaPrefix + "Atime"
	metaKeyMtime = metaPrefix + "Mtime"
	metaKeyCtime = metaPrefix + "Ctime"
)

// dirMarker is the zero-byte object suffix used to represent empty directories.
const dirMarker = "/.syncthing_dir_marker"

func init() {
	fs.RegisterFilesystemType(FilesystemTypeS3, func(uri string, opts ...fs.Option) (fs.Filesystem, error) {
		return NewS3Filesystem(uri, opts...)
	})
}

// S3Filesystem implements fs.Filesystem on top of an S3-compatible bucket.
//
// URI format:
//
//	s3://endpoint/bucket[/prefix][?accessKey=AK&secretKey=SK&useSSL=true]
//
// Credentials may be omitted from the URI and supplied via environment
// variables instead (see package comment above).
type S3Filesystem struct {
	client  *minio.Client
	bucket  string
	prefix  string // optional key prefix inside the bucket (always ends with "/" or is "")
	uri     string
	options []fs.Option
	// tree caches one recursive ListObjects of the folder prefix so scans
	// do not issue a ListObjects call per directory (costly on HDD backends).
	tree treeCache
}

// NewS3Filesystem creates a new S3-backed filesystem from a URI of the form:
//
//	s3://endpoint/bucket[/prefix][?accessKey=AK&secretKey=SK&useSSL=true]
//
// If accessKey/secretKey are not in the query string, they are read from the
// environment (S3_*, AWS_*, or MINIO_* variables).
func NewS3Filesystem(uri string, opts ...fs.Option) (*S3Filesystem, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("s3fs: invalid URI: %w", err)
	}

	endpoint := u.Host
	pathParts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(pathParts) == 0 || pathParts[0] == "" {
		return nil, fmt.Errorf("s3fs: bucket name missing in URI")
	}
	bucket := pathParts[0]
	prefix := ""
	if len(pathParts) > 1 && pathParts[1] != "" {
		prefix = strings.TrimSuffix(pathParts[1], "/") + "/"
	}

	accessKey, secretKey, sessionToken, useSSL, err := resolveS3Credentials(u.Query())
	if err != nil {
		return nil, err
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, sessionToken),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("s3fs: failed to create minio client: %w", err)
	}

	return &S3Filesystem{
		client:  client,
		bucket:  bucket,
		prefix:  prefix,
		uri:     redactS3URI(uri),
		options: opts,
	}, nil
}

// resolveS3Credentials returns access key, secret, optional session token, and
// whether to use TLS. Query parameters override environment variables.
func resolveS3Credentials(q url.Values) (accessKey, secretKey, sessionToken string, useSSL bool, err error) {
	accessKey = firstNonEmpty(
		q.Get("accessKey"),
		os.Getenv("S3_ACCESS_KEY_ID"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("MINIO_ACCESS_KEY"),
		os.Getenv("MINIO_ROOT_USER"),
	)
	secretKey = firstNonEmpty(
		q.Get("secretKey"),
		os.Getenv("S3_SECRET_ACCESS_KEY"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		os.Getenv("MINIO_SECRET_KEY"),
		os.Getenv("MINIO_ROOT_PASSWORD"),
	)
	sessionToken = firstNonEmpty(
		q.Get("sessionToken"),
		os.Getenv("S3_SESSION_TOKEN"),
		os.Getenv("AWS_SESSION_TOKEN"),
	)

	if _, ok := q["useSSL"]; ok {
		useSSL = q.Get("useSSL") == "true"
	} else {
		switch strings.ToLower(os.Getenv("S3_USE_SSL")) {
		case "1", "true", "yes":
			useSSL = true
		}
	}

	if accessKey == "" || secretKey == "" {
		return "", "", "", false, fmt.Errorf("s3fs: credentials missing: set accessKey/secretKey in the folder URI, or set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY (or AWS_* / MINIO_*) in the environment")
	}
	return accessKey, secretKey, sessionToken, useSSL, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// redactS3URI removes secrets from a URI so it is safe to log or show in the UI.
func redactS3URI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	q := u.Query()
	changed := false
	for _, key := range []string{"accessKey", "secretKey", "sessionToken"} {
		if q.Has(key) {
			q.Set(key, "***")
			changed = true
		}
	}
	if !changed {
		return uri
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// NewS3FilesystemFromClient creates an S3-backed filesystem with an externally provided
// MinIO client. Useful for testing.
func NewS3FilesystemFromClient(client *minio.Client, bucket, prefix string, opts ...fs.Option) *S3Filesystem {
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	return &S3Filesystem{
		client:  client,
		bucket:  bucket,
		prefix:  prefix,
		uri:     fmt.Sprintf("s3://%s/%s/%s", client.EndpointURL().Host, bucket, prefix),
		options: opts,
	}
}

// key returns the full S3 object key for a relative path.
func (f *S3Filesystem) key(name string) (string, error) {
	name, err := fs.Canonicalize(name)
	if err != nil {
		return "", err
	}
	if name == "." {
		return f.prefix, nil
	}
	return f.prefix + name, nil
}

// relPath strips the prefix from an S3 object key.
func (f *S3Filesystem) relPath(key string) string {
	rel := strings.TrimPrefix(key, f.prefix)
	rel = strings.TrimSuffix(rel, "/")
	rel = strings.TrimSuffix(rel, "/.syncthing_dir_marker")
	if rel == "" {
		return "."
	}
	return rel
}

// defaultMeta returns default POSIX metadata for a new object with the given mode.
func defaultMeta(mode fs.FileMode) map[string]string {
	now := time.Now()
	return map[string]string{
		metaKeyMode:  strconv.FormatUint(uint64(mode), 8),
		metaKeyUID:   "0",
		metaKeyGID:   "0",
		metaKeyAtime: now.Format(time.RFC3339Nano),
		metaKeyMtime: now.Format(time.RFC3339Nano),
		metaKeyCtime: now.Format(time.RFC3339Nano),
	}
}

// ── Filesystem interface ──

func (f *S3Filesystem) Chmod(name string, mode fs.FileMode) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}
	return f.updateMeta(k, func(m map[string]string) {
		m[metaKeyMode] = strconv.FormatUint(uint64(mode), 8)
	})
}

func (f *S3Filesystem) Lchown(name string, uid, gid string) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	// Check for the object itself or a dir marker.
	realKey, err := f.resolveKey(k)
	if err != nil {
		return err
	}

	return f.updateMeta(realKey, func(m map[string]string) {
		m[metaKeyUID] = uid
		m[metaKeyGID] = gid
	})
}

func (f *S3Filesystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	realKey, err := f.resolveKey(k)
	if err != nil {
		return err
	}

	return f.updateMeta(realKey, func(m map[string]string) {
		m[metaKeyAtime] = atime.Format(time.RFC3339Nano)
		m[metaKeyMtime] = mtime.Format(time.RFC3339Nano)
	})
}

func (f *S3Filesystem) Create(name string) (fs.File, error) {
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (f *S3Filesystem) CreateSymlink(target, name string) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}
	meta := defaultMeta(fs.ModeSymlink | 0o777)
	_, err = f.client.PutObject(context.Background(), f.bucket, k, strings.NewReader(target), int64(len(target)), minio.PutObjectOptions{
		UserMetadata: meta,
	})
	f.tree.invalidate()
	return err
}

func (f *S3Filesystem) DirNames(name string) ([]string, error) {
	if _, err := f.key(name); err != nil {
		return nil, err
	}

	// Prefer a single recursive listing of the folder prefix (paginated by
	// the SDK) over one ListObjects per directory. On HDD-backed MinIO this
	// is typically orders of magnitude cheaper during a full scan.
	if err := f.ensureTree(context.Background()); err != nil {
		return nil, err
	}
	if names, ok := f.cachedDirNames(name); ok {
		return names, nil
	}

	// Directory missing in cache → empty or not found; match previous
	// behavior of listing an empty prefix (empty result, not error).
	return []string{}, nil
}

func (f *S3Filesystem) Lstat(name string) (fs.FileInfo, error) {
	return f.stat(name)
}

func (f *S3Filesystem) Mkdir(name string, perm fs.FileMode) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	// Check if it already exists
	markerKey := k + dirMarker
	_, e := f.client.StatObject(context.Background(), f.bucket, markerKey, minio.StatObjectOptions{})
	if e == nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	}

	meta := defaultMeta(perm | fs.FileMode(os.ModeDir))
	_, err = f.client.PutObject(context.Background(), f.bucket, markerKey, bytes.NewReader(nil), 0, minio.PutObjectOptions{
		UserMetadata: meta,
	})
	f.tree.invalidate()
	return err
}

func (f *S3Filesystem) MkdirAll(name string, perm fs.FileMode) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	// Create each path component
	parts := strings.Split(strings.TrimSuffix(k, "/"), "/")
	for i := range parts {
		dir := strings.Join(parts[:i+1], "/")
		markerKey := dir + dirMarker
		_, e := f.client.StatObject(context.Background(), f.bucket, markerKey, minio.StatObjectOptions{})
		if e != nil {
			meta := defaultMeta(perm | fs.FileMode(os.ModeDir))
			_, err = f.client.PutObject(context.Background(), f.bucket, markerKey, bytes.NewReader(nil), 0, minio.PutObjectOptions{
				UserMetadata: meta,
			})
			if err != nil {
				return err
			}
		}
	}
	f.tree.invalidate()
	return nil
}

func (f *S3Filesystem) Open(name string) (fs.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *S3Filesystem) OpenFile(name string, flags int, mode fs.FileMode) (fs.File, error) {
	k, err := f.key(name)
	if err != nil {
		return nil, err
	}

	// Spill object content to a local temp file so large objects do not need
	// to sit entirely in process RAM (ReadAt/WriteAt/Seek still work).
	tmp, err := os.CreateTemp("", "syncthing-s3-*")
	if err != nil {
		return nil, fmt.Errorf("s3fs: create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	sf := &s3File{
		fs:      f,
		name:    name,
		key:     k,
		flags:   flags,
		mode:    mode,
		tmp:     tmp,
		tmpPath: tmpPath,
	}

	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	writable := flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0

	if writable {
		if flags&os.O_TRUNC != 0 || flags&os.O_CREATE != 0 {
			obj, statErr := f.client.StatObject(context.Background(), f.bucket, k, minio.StatObjectOptions{})
			if statErr == nil {
				sf.meta = obj.UserMetadata
			}
			if flags&os.O_EXCL != 0 && statErr == nil {
				cleanup()
				return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
			}
			if flags&os.O_TRUNC != 0 {
				// Empty local file; must upload even if never written.
				sf.dirty = true
			} else if statErr == nil {
				if err := sf.downloadToTemp(); err != nil {
					cleanup()
					return nil, err
				}
			} else {
				// New object.
				sf.dirty = true
			}
		} else {
			// Writable without create/trunc: object must already exist.
			if err := sf.downloadToTemp(); err != nil {
				cleanup()
				return nil, err
			}
		}
	} else {
		// Read-only.
		if err := sf.downloadToTemp(); err != nil {
			cleanup()
			return nil, err
		}
	}

	if flags&os.O_APPEND != 0 {
		if _, err := sf.tmp.Seek(0, io.SeekEnd); err != nil {
			cleanup()
			return nil, err
		}
	} else {
		if _, err := sf.tmp.Seek(0, io.SeekStart); err != nil {
			cleanup()
			return nil, err
		}
	}

	return sf, nil
}

func (f *S3Filesystem) ReadSymlink(name string) (string, error) {
	k, err := f.key(name)
	if err != nil {
		return "", err
	}
	obj, err := f.client.GetObject(context.Background(), f.bucket, k, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (f *S3Filesystem) Remove(name string) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	// Try removing as file first
	err = f.client.RemoveObject(context.Background(), f.bucket, k, minio.RemoveObjectOptions{})
	if err != nil {
		// Try as directory marker
		err = f.client.RemoveObject(context.Background(), f.bucket, k+dirMarker, minio.RemoveObjectOptions{})
		f.tree.invalidate()
		return err
	}
	// Also try removing the dir marker if any
	_ = f.client.RemoveObject(context.Background(), f.bucket, k+dirMarker, minio.RemoveObjectOptions{})
	f.tree.invalidate()
	return nil
}

func (f *S3Filesystem) RemoveAll(name string) error {
	k, err := f.key(name)
	if err != nil {
		return err
	}

	prefix := k
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	ctx := context.Background()

	// Remove the object itself if it's a file
	_ = f.client.RemoveObject(ctx, f.bucket, k, minio.RemoveObjectOptions{})

	// Remove all objects under this prefix
	objectsCh := f.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			return obj.Err
		}
		err := f.client.RemoveObject(ctx, f.bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}
	}
	f.tree.invalidate()
	return nil
}

func (f *S3Filesystem) Rename(oldname, newname string) error {
	oldKey, err := f.key(oldname)
	if err != nil {
		return err
	}
	newKey, err := f.key(newname)
	if err != nil {
		return err
	}

	ctx := context.Background()
	src := minio.CopySrcOptions{
		Bucket: f.bucket,
		Object: oldKey,
	}
	dst := minio.CopyDestOptions{
		Bucket: f.bucket,
		Object: newKey,
	}
	_, err = f.client.CopyObject(ctx, dst, src)
	if err != nil {
		return err
	}

	err = f.client.RemoveObject(ctx, f.bucket, oldKey, minio.RemoveObjectOptions{})
	f.tree.invalidate()
	return err
}

func (f *S3Filesystem) Stat(name string) (fs.FileInfo, error) {
	return f.stat(name)
}

func (f *S3Filesystem) stat(name string) (fs.FileInfo, error) {
	if _, err := f.key(name); err != nil {
		return nil, err
	}

	// Serve from tree cache when warm (scan path). Avoids HeadObject per file
	// and ListObjects for directory existence checks.
	if err := f.ensureTree(context.Background()); err == nil {
		if fi, ok := f.cachedStat(name); ok {
			return fi, nil
		}
		// Cache is authoritative while valid: missing means not found.
		if f.treeHasLoaded() {
			return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
		}
	}

	k, err := f.key(name)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	// For "." (root), check if the prefix exists at all by listing objects.
	if name == "." || k == f.prefix {
		return f.statDir(name, k)
	}

	// Try as file first.
	obj, err := f.client.StatObject(ctx, f.bucket, k, minio.StatObjectOptions{})
	if err == nil {
		return newS3FileInfo(name, obj.Size, obj.UserMetadata, obj.LastModified), nil
	}

	// Try as directory (look for dir marker or any child objects).
	return f.statDir(name, k)
}

func (f *S3Filesystem) treeHasLoaded() bool {
	f.tree.mu.Lock()
	defer f.tree.mu.Unlock()
	return f.tree.validLocked()
}

func (f *S3Filesystem) statDir(name, k string) (fs.FileInfo, error) {
	ctx := context.Background()

	// For the root directory (name == "."), the bucket itself is the directory.
	if name == "." || k == f.prefix || k == "" {
		// Check for a dir marker at the root.
		markerKey := f.prefix + ".syncthing_dir_marker"
		obj, err := f.client.StatObject(ctx, f.bucket, markerKey, minio.StatObjectOptions{})
		if err == nil {
			meta := obj.UserMetadata
			if meta == nil {
				meta = make(map[string]string)
			}
			if _, ok := meta[metaKeyMode]; !ok {
				meta[metaKeyMode] = strconv.FormatUint(uint64(os.ModeDir|0o755), 8)
			}
			return newS3FileInfo(".", 0, meta, obj.LastModified), nil
		}
		// Even without a dir marker the root always exists as long as the bucket does.
		meta := map[string]string{
			metaKeyMode: strconv.FormatUint(uint64(os.ModeDir|0o755), 8),
		}
		return newS3FileInfo(".", 0, meta, time.Now()), nil
	}

	// Check for a dir marker.
	markerKey := k + dirMarker
	if strings.HasSuffix(k, "/") {
		markerKey = k + ".syncthing_dir_marker"
	}
	obj, err := f.client.StatObject(ctx, f.bucket, markerKey, minio.StatObjectOptions{})
	if err == nil {
		meta := obj.UserMetadata
		if meta == nil {
			meta = make(map[string]string)
		}
		// Force directory mode
		if _, ok := meta[metaKeyMode]; !ok {
			meta[metaKeyMode] = strconv.FormatUint(uint64(os.ModeDir|0o755), 8)
		}
		return newS3FileInfo(name, 0, meta, obj.LastModified), nil
	}

	// Check for any children.
	prefix := k
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	for obj := range f.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
		MaxKeys:   1,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		// Found at least one child; it's a directory.
		meta := map[string]string{
			metaKeyMode: strconv.FormatUint(uint64(os.ModeDir|0o755), 8),
		}
		return newS3FileInfo(name, 0, meta, time.Now()), nil
	}

	return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
}

func (*S3Filesystem) Walk(_ string, _ fs.WalkFunc) error {
	// Handled by WalkFilesystem wrapper
	return errors.New("not implemented")
}

func (*S3Filesystem) Watch(_ string, _ fs.Matcher, _ context.Context, _ bool) (<-chan fs.Event, <-chan error, error) {
	return nil, nil, fs.ErrWatchNotSupported
}

func (*S3Filesystem) Hide(_ string) error {
	return nil // no-op on S3
}

func (*S3Filesystem) Unhide(_ string) error {
	return nil // no-op on S3
}

func (f *S3Filesystem) Glob(pattern string) ([]string, error) {
	// Simple implementation: list all objects and match against the pattern.
	k, err := f.key(".")
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var matches []string
	for obj := range f.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
		Prefix:    k,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		rel := f.relPath(obj.Key)
		if rel == "." {
			continue
		}
		matched, err := path.Match(pattern, rel)
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, rel)
		}
	}
	return matches, nil
}

func (*S3Filesystem) Roots() ([]string, error) {
	return []string{"/"}, nil
}

func (*S3Filesystem) Usage(_ string) (fs.Usage, error) {
	// S3 doesn't have a concept of disk usage in the traditional sense.
	// Return "unlimited" free space.
	return fs.Usage{
		Free:  1 << 62,
		Total: 1 << 62,
	}, nil
}

func (*S3Filesystem) Type() fs.FilesystemType {
	return FilesystemTypeS3
}

func (f *S3Filesystem) URI() string {
	return f.uri
}

func (f *S3Filesystem) Options() []fs.Option {
	return f.options
}

func (*S3Filesystem) SameFile(fi1, fi2 fs.FileInfo) bool {
	s1, ok1 := fi1.(*s3FileInfo)
	s2, ok2 := fi2.(*s3FileInfo)
	if !ok1 || !ok2 {
		return false
	}
	return s1.name == s2.name && s1.modTime.Equal(s2.modTime) && s1.size == s2.size
}

func (*S3Filesystem) PlatformData(_ string, _, _ bool, _ fs.XattrFilter) (protocol.PlatformData, error) {
	return protocol.PlatformData{}, nil
}

func (*S3Filesystem) GetXattr(_ string, _ fs.XattrFilter) ([]protocol.Xattr, error) {
	return nil, fs.ErrXattrsNotSupported
}

func (*S3Filesystem) SetXattr(_ string, _ []protocol.Xattr, _ fs.XattrFilter) error {
	return fs.ErrXattrsNotSupported
}

func (*S3Filesystem) underlying() (fs.Filesystem, bool) {
	return nil, false
}

// ── Helper methods ──

// resolveKey returns the actual S3 key for a given prefix, checking both
// the key itself and its directory marker variant.
func (f *S3Filesystem) resolveKey(k string) (string, error) {
	ctx := context.Background()
	_, err := f.client.StatObject(ctx, f.bucket, k, minio.StatObjectOptions{})
	if err == nil {
		return k, nil
	}
	markerKey := k + dirMarker
	_, err = f.client.StatObject(ctx, f.bucket, markerKey, minio.StatObjectOptions{})
	if err == nil {
		return markerKey, nil
	}
	return "", &os.PathError{Op: "stat", Path: k, Err: os.ErrNotExist}
}

// updateMeta reads the current object, modifies its metadata, and writes it back.
func (f *S3Filesystem) updateMeta(key string, fn func(map[string]string)) error {
	ctx := context.Background()
	obj, err := f.client.StatObject(ctx, f.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return err
	}

	meta := obj.UserMetadata
	if meta == nil {
		meta = make(map[string]string)
	}
	fn(meta)

	// Use CopyObject to update metadata in-place (server-side copy).
	src := minio.CopySrcOptions{
		Bucket: f.bucket,
		Object: key,
	}
	dst := minio.CopyDestOptions{
		Bucket:          f.bucket,
		Object:          key,
		UserMetadata:    meta,
		ReplaceMetadata: true,
	}
	_, err = f.client.CopyObject(ctx, dst, src)
	// Metadata (mode/mtime) may change what scanners care about.
	f.tree.invalidate()
	return err
}

// ── s3FileInfo ──

type s3FileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	uid     int
	gid     int
}

func newS3FileInfo(name string, size int64, meta map[string]string, lastModified time.Time) *s3FileInfo {
	fi := &s3FileInfo{
		name:    path.Base(name),
		size:    size,
		mode:    0o644,
		modTime: lastModified,
	}

	if name == "." {
		fi.name = "."
	}

	if v, ok := meta[metaKeyMode]; ok {
		if m, err := strconv.ParseUint(v, 8, 32); err == nil {
			fi.mode = fs.FileMode(m)
		}
	}
	if v, ok := meta[metaKeyUID]; ok {
		if uid, err := strconv.Atoi(v); err == nil {
			fi.uid = uid
		}
	}
	if v, ok := meta[metaKeyGID]; ok {
		if gid, err := strconv.Atoi(v); err == nil {
			fi.gid = gid
		}
	}
	if v, ok := meta[metaKeyMtime]; ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			fi.modTime = t
		}
	}

	return fi
}

func (fi *s3FileInfo) Name() string        { return fi.name }
func (fi *s3FileInfo) Size() int64         { return fi.size }
func (fi *s3FileInfo) Mode() fs.FileMode   { return fi.mode }
func (fi *s3FileInfo) ModTime() time.Time  { return fi.modTime }
func (fi *s3FileInfo) IsDir() bool         { return fi.mode&fs.FileMode(os.ModeDir) != 0 }
func (fi *s3FileInfo) Sys() interface{}    { return nil }
func (fi *s3FileInfo) IsRegular() bool     { return fi.mode&fs.ModeType == 0 }
func (fi *s3FileInfo) IsSymlink() bool     { return fi.mode&fs.ModeSymlink != 0 }
func (fi *s3FileInfo) Owner() int          { return fi.uid }
func (fi *s3FileInfo) Group() int          { return fi.gid }

// ── s3File ──

// s3File implements fs.File using a local temporary file as the backing store.
// Object content is streamed from S3 into the temp file on open (when needed)
// and streamed back with PutObject on flush/close if dirty. This keeps peak
// memory use low for large objects while still supporting Seek/ReadAt/WriteAt.
type s3File struct {
	fs      *S3Filesystem
	name    string
	key     string
	flags   int
	mode    fs.FileMode
	tmp     *os.File
	tmpPath string
	dirty   bool
	closed  bool
	mu      sync.Mutex
	meta    map[string]string
}

// downloadToTemp streams the S3 object into the temp file and captures metadata.
func (f *s3File) downloadToTemp() error {
	ctx := context.Background()
	obj, err := f.fs.client.GetObject(ctx, f.fs.bucket, f.key, minio.GetObjectOptions{})
	if err != nil {
		return &os.PathError{Op: "open", Path: f.name, Err: os.ErrNotExist}
	}
	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		return &os.PathError{Op: "open", Path: f.name, Err: os.ErrNotExist}
	}

	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := f.tmp.Truncate(0); err != nil {
		return err
	}
	if _, err := io.Copy(f.tmp, obj); err != nil {
		return err
	}
	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	f.meta = info.UserMetadata
	return nil
}

func (f *s3File) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.tmp.Read(p)
}

func (f *s3File) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.tmp.ReadAt(p, off)
}

func (f *s3File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	n, err := f.tmp.Write(p)
	if n > 0 {
		f.dirty = true
	}
	return n, err
}

func (f *s3File) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	n, err := f.tmp.WriteAt(p, off)
	if n > 0 {
		f.dirty = true
	}
	return n, err
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.tmp.Seek(offset, whence)
}

func (f *s3File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true

	var flushErr error
	if f.dirty && f.flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		flushErr = f.flushLocked()
	}

	// Always release the temp file.
	_ = f.tmp.Close()
	_ = os.Remove(f.tmpPath)
	f.tmp = nil
	return flushErr
}

// flushLocked uploads the temp file to S3. Caller must hold f.mu.
func (f *s3File) flushLocked() error {
	meta := f.meta
	if meta == nil {
		meta = defaultMeta(f.mode)
	}
	meta[metaKeyMtime] = time.Now().Format(time.RFC3339Nano)

	fi, err := f.tmp.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Stream from disk; minio-go will multipart large objects as needed.
	_, err = f.fs.client.PutObject(context.Background(), f.fs.bucket, f.key, f.tmp, size, minio.PutObjectOptions{
		UserMetadata: meta,
	})
	if err != nil {
		return err
	}
	f.dirty = false
	f.fs.tree.invalidate()
	return nil
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if err := f.tmp.Truncate(size); err != nil {
		return err
	}
	// Match os.File: truncation does not move the offset by itself.
	f.dirty = true
	return nil
}

func (f *s3File) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, os.ErrClosed
	}

	meta := f.meta
	if meta == nil {
		meta = defaultMeta(f.mode)
	}
	fi, err := f.tmp.Stat()
	if err != nil {
		return nil, err
	}
	return newS3FileInfo(f.name, fi.Size(), meta, fi.ModTime()), nil
}

func (f *s3File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if err := f.tmp.Sync(); err != nil {
		return err
	}
	if f.dirty {
		return f.flushLocked()
	}
	return nil
}

// Ensure interface compliance
var _ fs.Filesystem = (*S3Filesystem)(nil)
var _ fs.File = (*s3File)(nil)
var _ fs.FileInfo = (*s3FileInfo)(nil)

// Silence the unused import warning for slog
var _ = slog.Debug
