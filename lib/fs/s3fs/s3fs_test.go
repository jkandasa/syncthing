// Copyright (C) 2024 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package s3fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/syncthing/syncthing/lib/fs"
)

// testS3Env holds the MinIO connection details for integration tests.
// Tests are skipped if the environment variables are not set.
type testS3Env struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
}

func getTestEnv(t *testing.T) testS3Env {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
	secretKey := os.Getenv("S3_TEST_SECRET_KEY")
	bucket := os.Getenv("S3_TEST_BUCKET")

	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	if bucket == "" {
		bucket = fmt.Sprintf("syncthing-test-%d", rand.Int63())
	}

	return testS3Env{
		endpoint:  endpoint,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
	}
}

func newTestFS(t *testing.T) *S3Filesystem {
	t.Helper()
	env := getTestEnv(t)

	client, err := minio.New(env.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(env.accessKey, env.secretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("Failed to create MinIO client: %v", err)
	}

	// Check connectivity
	ctx := context.Background()
	_, err = client.ListBuckets(ctx)
	if err != nil {
		t.Skipf("MinIO not available at %s: %v (set S3_TEST_ENDPOINT to enable)", env.endpoint, err)
	}

	// Create test bucket
	err = client.MakeBucket(ctx, env.bucket, minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := client.BucketExists(ctx, env.bucket)
		if errBucketExists != nil || !exists {
			t.Fatalf("Failed to create test bucket: %v", err)
		}
	}

	t.Cleanup(func() {
		// Remove all objects
		for obj := range client.ListObjects(ctx, env.bucket, minio.ListObjectsOptions{Recursive: true}) {
			_ = client.RemoveObject(ctx, env.bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		_ = client.RemoveBucket(ctx, env.bucket)
	})

	return NewS3FilesystemFromClient(client, env.bucket, "", nil)
}

func TestS3FSType(t *testing.T) {
	sfs := newTestFS(t)
	if sfs.Type() != FilesystemTypeS3 {
		t.Errorf("expected type %q, got %q", FilesystemTypeS3, sfs.Type())
	}
}

func TestCreateAndReadFile(t *testing.T) {
	sfs := newTestFS(t)

	// Create a file
	f, err := sfs.Create("hello.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	content := []byte("Hello, S3 World!")
	_, err = f.Write(content)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read it back
	f2, err := sfs.Open("hello.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f2.Close()

	data, err := io.ReadAll(f2)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}
}

func TestMkdirAndDirNames(t *testing.T) {
	sfs := newTestFS(t)

	// Create a directory
	if err := sfs.Mkdir("testdir", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	// Create a file in the directory
	f, err := sfs.Create("testdir/file1.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("content1"))
	f.Close()

	f, err = sfs.Create("testdir/file2.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("content2"))
	f.Close()

	// List directory names
	names, err := sfs.DirNames("testdir")
	if err != nil {
		t.Fatalf("DirNames: %v", err)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "file1.txt" || names[1] != "file2.txt" {
		t.Errorf("DirNames = %v, want [file1.txt file2.txt]", names)
	}
}

func TestMkdirAll(t *testing.T) {
	sfs := newTestFS(t)

	if err := sfs.MkdirAll("a/b/c", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Verify directory exists by statting it
	fi, err := sfs.Stat("a/b/c")
	if err != nil {
		t.Fatalf("Stat a/b/c: %v", err)
	}
	if !fi.IsDir() {
		t.Error("expected a/b/c to be a directory")
	}
}

func TestStat(t *testing.T) {
	sfs := newTestFS(t)

	content := []byte("stat test data")
	f, err := sfs.Create("statfile.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write(content)
	f.Close()

	fi, err := sfs.Stat("statfile.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Name() != "statfile.txt" {
		t.Errorf("Name = %q, want %q", fi.Name(), "statfile.txt")
	}
	if fi.Size() != int64(len(content)) {
		t.Errorf("Size = %d, want %d", fi.Size(), len(content))
	}
	if fi.IsDir() {
		t.Error("expected file, not dir")
	}
}

func TestStatDirectory(t *testing.T) {
	sfs := newTestFS(t)

	if err := sfs.Mkdir("mydir", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	fi, err := sfs.Stat("mydir")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.IsDir() {
		t.Error("expected directory")
	}
}

func TestRemove(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("removeme.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("to be removed"))
	f.Close()

	if err := sfs.Remove("removeme.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	_, err = sfs.Stat("removeme.txt")
	if !fs.IsNotExist(err) {
		t.Errorf("expected NotExist error after Remove, got %v", err)
	}
}

func TestRemoveAll(t *testing.T) {
	sfs := newTestFS(t)

	if err := sfs.MkdirAll("dir1/dir2", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	f, err := sfs.Create("dir1/dir2/file.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("deep file"))
	f.Close()

	if err := sfs.RemoveAll("dir1"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	_, err = sfs.Stat("dir1")
	if !fs.IsNotExist(err) {
		t.Errorf("expected NotExist after RemoveAll, got %v", err)
	}
}

func TestRename(t *testing.T) {
	sfs := newTestFS(t)

	content := []byte("rename content")
	f, err := sfs.Create("oldname.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write(content)
	f.Close()

	if err := sfs.Rename("oldname.txt", "newname.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Old name should not exist
	_, err = sfs.Stat("oldname.txt")
	if !fs.IsNotExist(err) {
		t.Errorf("old name still exists after rename")
	}

	// Read with new name
	f2, err := sfs.Open("newname.txt")
	if err != nil {
		t.Fatalf("Open newname: %v", err)
	}
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch after rename: got %q, want %q", data, content)
	}
}

func TestChtimes(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("times.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("time test"))
	f.Close()

	now := time.Now().Truncate(time.Second)
	atime := now.Add(-1 * time.Hour)
	mtime := now.Add(-2 * time.Hour)

	if err := sfs.Chtimes("times.txt", atime, mtime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	fi, err := sfs.Stat("times.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// The mtime should match what we set (within a second tolerance for RFC3339 parsing)
	diff := fi.ModTime().Sub(mtime)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("ModTime diff = %v, want ~0", diff)
	}
}

func TestChmod(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("chmod.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("chmod test"))
	f.Close()

	if err := sfs.Chmod("chmod.txt", 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	fi, err := sfs.Stat("chmod.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if fi.Mode()&fs.ModePerm != 0o755 {
		t.Errorf("Mode = %o, want 755", fi.Mode()&fs.ModePerm)
	}
}

func TestLchown(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("chown.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("chown test"))
	f.Close()

	if err := sfs.Lchown("chown.txt", "1000", "1000"); err != nil {
		t.Fatalf("Lchown: %v", err)
	}

	fi, err := sfs.Stat("chown.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Owner() != 1000 {
		t.Errorf("Owner = %d, want 1000", fi.Owner())
	}
	if fi.Group() != 1000 {
		t.Errorf("Group = %d, want 1000", fi.Group())
	}
}

func TestFileSeek(t *testing.T) {
	sfs := newTestFS(t)

	content := []byte("0123456789")
	f, err := sfs.Create("seek.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write(content)
	f.Close()

	f2, err := sfs.Open("seek.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f2.Close()

	// Seek to position 5
	pos, err := f2.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if pos != 5 {
		t.Errorf("Seek pos = %d, want 5", pos)
	}

	buf := make([]byte, 5)
	n, err := f2.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read after seek: %v", err)
	}
	if !bytes.Equal(buf[:n], []byte("56789")) {
		t.Errorf("Read after seek = %q, want %q", buf[:n], "56789")
	}
}

func TestReadAt(t *testing.T) {
	sfs := newTestFS(t)

	content := []byte("Hello, ReadAt World!")
	f, err := sfs.Create("readat.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write(content)
	f.Close()

	f2, err := sfs.Open("readat.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f2.Close()

	buf := make([]byte, 6)
	n, err := f2.ReadAt(buf, 7)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(buf[:n], []byte("ReadAt")) {
		t.Errorf("ReadAt = %q, want %q", buf[:n], "ReadAt")
	}
}

func TestWriteAt(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.OpenFile("writeat.txt", os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	_, _ = f.Write([]byte("Hello, World!"))
	_, _ = f.WriteAt([]byte("S3"), 7)
	f.Close()

	f2, err := sfs.Open("writeat.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "Hello, S3rld!" {
		t.Errorf("WriteAt result = %q, want %q", string(data), "Hello, S3rld!")
	}
}

func TestTruncate(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.OpenFile("truncate.txt", os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	_, _ = f.Write([]byte("Hello, World!"))
	if err := f.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	f.Close()

	f2, err := sfs.Open("truncate.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "Hello" {
		t.Errorf("Truncate result = %q, want %q", string(data), "Hello")
	}
}

func TestGlob(t *testing.T) {
	sfs := newTestFS(t)

	for _, name := range []string{"a.txt", "b.txt", "c.log"} {
		f, err := sfs.Create(name)
		if err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
		_, _ = f.Write([]byte("content"))
		f.Close()
	}

	matches, err := sfs.Glob("*.txt")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	sort.Strings(matches)
	if len(matches) != 2 || matches[0] != "a.txt" || matches[1] != "b.txt" {
		t.Errorf("Glob = %v, want [a.txt b.txt]", matches)
	}
}

func TestCreateSymlink(t *testing.T) {
	sfs := newTestFS(t)

	// Create a file first
	f, err := sfs.Create("target.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("target content"))
	f.Close()

	// Create symlink
	if err := sfs.CreateSymlink("target.txt", "link.txt"); err != nil {
		t.Fatalf("CreateSymlink: %v", err)
	}

	// Read symlink
	target, err := sfs.ReadSymlink("link.txt")
	if err != nil {
		t.Fatalf("ReadSymlink: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("ReadSymlink = %q, want %q", target, "target.txt")
	}
}

func TestUsage(t *testing.T) {
	sfs := newTestFS(t)

	usage, err := sfs.Usage(".")
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if usage.Free == 0 || usage.Total == 0 {
		t.Errorf("Usage returned zero values: %+v", usage)
	}
}

func TestSameFile(t *testing.T) {
	sfs := newTestFS(t)

	f, _ := sfs.Create("same.txt")
	_, _ = f.Write([]byte("data"))
	f.Close()

	fi1, _ := sfs.Stat("same.txt")
	fi2, _ := sfs.Stat("same.txt")

	if !sfs.SameFile(fi1, fi2) {
		t.Error("SameFile returned false for same file")
	}
}

func TestOpenFileExclusive(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("excl.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("exists"))
	f.Close()

	_, err = sfs.OpenFile("excl.txt", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o666)
	if err == nil {
		t.Error("expected error for O_EXCL on existing file")
	}
}

func TestOpenFileAppend(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("append.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("Hello"))
	f.Close()

	f2, err := sfs.OpenFile("append.txt", os.O_RDWR|os.O_APPEND, 0o666)
	if err != nil {
		t.Fatalf("OpenFile append: %v", err)
	}
	_, _ = f2.Write([]byte(", World!"))
	f2.Close()

	f3, err := sfs.Open("append.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f3.Close()
	data, _ := io.ReadAll(f3)
	if string(data) != "Hello, World!" {
		t.Errorf("Append result = %q, want %q", string(data), "Hello, World!")
	}
}

func TestStatRootDir(t *testing.T) {
	sfs := newTestFS(t)

	// Create something so the bucket is not empty
	if err := sfs.Mkdir("roottest", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	fi, err := sfs.Stat(".")
	if err != nil {
		t.Fatalf("Stat root: %v", err)
	}
	if !fi.IsDir() {
		t.Error("root should be a directory")
	}
}

func TestWalkNotImplemented(t *testing.T) {
	sfs := newTestFS(t)
	err := sfs.Walk(".", func(path string, info fs.FileInfo, err error) error {
		return nil
	})
	if err == nil {
		t.Error("expected error from Walk (should be 'not implemented')")
	}
}

func TestWatchNotSupported(t *testing.T) {
	sfs := newTestFS(t)
	_, _, err := sfs.Watch(".", nil, context.Background(), false)
	if err != fs.ErrWatchNotSupported {
		t.Errorf("Watch should return ErrWatchNotSupported, got %v", err)
	}
}

func TestMetadataPreservation(t *testing.T) {
	sfs := newTestFS(t)

	// Create a file
	f, err := sfs.Create("meta.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("metadata test"))
	f.Close()

	// Set permissions
	if err := sfs.Chmod("meta.txt", 0o644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	// Set ownership
	if err := sfs.Lchown("meta.txt", "500", "500"); err != nil {
		t.Fatalf("Lchown: %v", err)
	}

	// Set times
	mtime := time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)
	atime := time.Date(2023, 6, 15, 13, 0, 0, 0, time.UTC)
	if err := sfs.Chtimes("meta.txt", atime, mtime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	// Verify all metadata
	fi, err := sfs.Stat("meta.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if fi.Mode()&fs.ModePerm != 0o644 {
		t.Errorf("permissions: got %o, want 644", fi.Mode()&fs.ModePerm)
	}
	if fi.Owner() != 500 {
		t.Errorf("owner: got %d, want 500", fi.Owner())
	}
	if fi.Group() != 500 {
		t.Errorf("group: got %d, want 500", fi.Group())
	}

	diff := fi.ModTime().Sub(mtime)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("mtime: got %v, want %v (diff %v)", fi.ModTime(), mtime, diff)
	}
}

func TestMkdirAlreadyExists(t *testing.T) {
	sfs := newTestFS(t)

	if err := sfs.Mkdir("existing", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	err := sfs.Mkdir("existing", 0o755)
	if err == nil {
		t.Error("expected error when creating existing directory")
	}
}

func TestFileStat(t *testing.T) {
	sfs := newTestFS(t)

	f, err := sfs.Create("filestat.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _ = f.Write([]byte("file stat test"))

	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("file Stat: %v", err)
	}

	if fi.Size() != 14 {
		t.Errorf("Size = %d, want 14", fi.Size())
	}

	f.Close()
}

func TestNewS3FilesystemFromURI(t *testing.T) {
	// Just test URI parsing, no actual connection required.
	uri := "s3://localhost:9000/mybucket/myprefix?accessKey=AK&secretKey=SK&useSSL=false"
	sfs, err := NewS3Filesystem(uri)
	if err != nil {
		t.Fatalf("NewS3Filesystem: %v", err)
	}
	if sfs.bucket != "mybucket" {
		t.Errorf("bucket = %q, want %q", sfs.bucket, "mybucket")
	}
	if sfs.prefix != "myprefix/" {
		t.Errorf("prefix = %q, want %q", sfs.prefix, "myprefix/")
	}
}

func TestNewS3FilesystemFromURINoBucket(t *testing.T) {
	uri := "s3://localhost:9000/?accessKey=AK&secretKey=SK"
	_, err := NewS3Filesystem(uri)
	if err == nil {
		t.Error("expected error for URI without bucket name")
	}
}

func TestFilesystemTypeRegistration(t *testing.T) {
	// Verify that the filesystem type is registered and can create instances.
	// This tests the init() function.
	fsSys := fs.NewFilesystem(FilesystemTypeS3, "s3://localhost:9000/test?accessKey=AK&secretKey=SK")
	if fsSys.Type() != FilesystemTypeS3 {
		t.Errorf("type = %q, want %q", fsSys.Type(), FilesystemTypeS3)
	}
}

func TestHideUnhideNoOp(t *testing.T) {
	sfs := newTestFS(t)
	if err := sfs.Hide("anything"); err != nil {
		t.Errorf("Hide should be no-op, got error: %v", err)
	}
	if err := sfs.Unhide("anything"); err != nil {
		t.Errorf("Unhide should be no-op, got error: %v", err)
	}
}

func TestDirNamesSubdirectories(t *testing.T) {
	sfs := newTestFS(t)

	// Create subdirectories under parent
	if err := sfs.Mkdir("parent", 0o755); err != nil {
		t.Fatalf("Mkdir parent: %v", err)
	}
	if err := sfs.Mkdir("parent/child1", 0o755); err != nil {
		t.Fatalf("Mkdir child1: %v", err)
	}
	if err := sfs.Mkdir("parent/child2", 0o755); err != nil {
		t.Fatalf("Mkdir child2: %v", err)
	}

	f, _ := sfs.Create("parent/file.txt")
	_, _ = f.Write([]byte("hello"))
	f.Close()

	names, err := sfs.DirNames("parent")
	if err != nil {
		t.Fatalf("DirNames: %v", err)
	}
	sort.Strings(names)

	expected := []string{"child1", "child2", "file.txt"}
	if len(names) != len(expected) {
		t.Fatalf("DirNames = %v, want %v", names, expected)
	}
	for i := range expected {
		if names[i] != expected[i] {
			t.Errorf("DirNames[%d] = %q, want %q", i, names[i], expected[i])
		}
	}
}
