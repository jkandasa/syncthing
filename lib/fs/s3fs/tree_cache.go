// Copyright (C) 2024 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package s3fs

import (
	"context"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/syncthing/syncthing/lib/fs"
)

// treeCacheTTL is how long a full-prefix listing may be reused for DirNames
// and Lstat. Mutations always invalidate immediately. A short TTL keeps scan
// walks on HDD/object backends from re-listing every directory.
const treeCacheTTL = 2 * time.Minute

// treeNode is one path component in a cached listing of the bucket prefix.
type treeNode struct {
	// children maps base name -> node (files and subdirectories).
	children map[string]*treeNode
	isDir    bool
	size     int64
	modTime  time.Time
	// present is true if this path exists (as object, dir marker, or implied prefix).
	present bool
}

func newTreeNode() *treeNode {
	return &treeNode{children: make(map[string]*treeNode)}
}

// treeCache holds one recursive listing of the filesystem root prefix.
type treeCache struct {
	mu      sync.Mutex
	root    *treeNode
	expires time.Time
	// loaded is true if root is a successful listing (may be empty).
	loaded bool
}

func (c *treeCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.root = nil
	c.loaded = false
	c.expires = time.Time{}
}

func (c *treeCache) validLocked() bool {
	return c.loaded && time.Now().Before(c.expires)
}

// ensureTree loads a recursive listing of f.prefix if the cache is cold/expired.
func (f *S3Filesystem) ensureTree(ctx context.Context) error {
	f.tree.mu.Lock()
	if f.tree.validLocked() {
		f.tree.mu.Unlock()
		return nil
	}
	f.tree.mu.Unlock()

	root, err := f.buildTree(ctx)
	if err != nil {
		return err
	}

	f.tree.mu.Lock()
	// Another goroutine may have refreshed while we listed; keep the newer one.
	if !f.tree.validLocked() {
		f.tree.root = root
		f.tree.loaded = true
		f.tree.expires = time.Now().Add(treeCacheTTL)
	}
	f.tree.mu.Unlock()
	return nil
}

func (f *S3Filesystem) buildTree(ctx context.Context) (*treeNode, error) {
	root := newTreeNode()
	root.isDir = true
	root.present = true

	prefix := f.prefix // may be ""
	for obj := range f.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		rel := strings.TrimPrefix(obj.Key, prefix)
		if rel == "" {
			continue
		}

		isMarker := strings.HasSuffix(rel, ".syncthing_dir_marker") ||
			strings.HasSuffix(rel, dirMarker) ||
			rel == ".syncthing_dir_marker"
		if isMarker {
			// "dir/.syncthing_dir_marker" or ".syncthing_dir_marker"
			dirRel := strings.TrimSuffix(rel, ".syncthing_dir_marker")
			dirRel = strings.TrimSuffix(dirRel, "/")
			if dirRel == "" {
				// root marker
				root.modTime = obj.LastModified
				continue
			}
			n := root.ensurePath(dirRel, true)
			n.isDir = true
			n.present = true
			n.size = 0
			if obj.LastModified.After(n.modTime) {
				n.modTime = obj.LastModified
			}
			continue
		}

		// Regular object: create intermediate dirs and the file node.
		n := root.ensurePath(rel, false)
		n.isDir = false
		n.present = true
		n.size = obj.Size
		n.modTime = obj.LastModified
	}

	return root, nil
}

// ensurePath creates intermediate directory nodes and returns the node for rel.
// If asDir is true, the leaf is a directory; otherwise a file (parents are dirs).
func (n *treeNode) ensurePath(rel string, asDir bool) *treeNode {
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return n
	}
	parts := strings.Split(rel, "/")
	cur := n
	for i, p := range parts {
		if p == "" {
			continue
		}
		child, ok := cur.children[p]
		if !ok {
			child = newTreeNode()
			cur.children[p] = child
		}
		isLast := i == len(parts)-1
		if !isLast || asDir {
			child.isDir = true
			child.present = true
		}
		cur = child
	}
	return cur
}

func (n *treeNode) lookup(rel string) *treeNode {
	rel = strings.Trim(rel, "/")
	if rel == "" || rel == "." {
		return n
	}
	cur := n
	for _, p := range strings.Split(rel, "/") {
		if p == "" || p == "." {
			continue
		}
		child, ok := cur.children[p]
		if !ok {
			return nil
		}
		cur = child
	}
	// Implied directory: has children but was never a real object/marker.
	if !cur.present && len(cur.children) > 0 {
		cur.isDir = true
		cur.present = true
		return cur
	}
	if !cur.present {
		return nil
	}
	return cur
}

func (f *S3Filesystem) cachedDirNames(name string) ([]string, bool) {
	f.tree.mu.Lock()
	defer f.tree.mu.Unlock()
	if !f.tree.validLocked() || f.tree.root == nil {
		return nil, false
	}
	node := f.tree.root.lookup(name)
	if node == nil || !node.isDir {
		return nil, false
	}
	names := make([]string, 0, len(node.children))
	for n, ch := range node.children {
		if !ch.present && len(ch.children) == 0 {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	return names, true
}

func (f *S3Filesystem) cachedStat(name string) (fs.FileInfo, bool) {
	f.tree.mu.Lock()
	defer f.tree.mu.Unlock()
	if !f.tree.validLocked() || f.tree.root == nil {
		return nil, false
	}
	node := f.tree.root.lookup(name)
	if node == nil {
		return nil, false
	}
	if !node.present && len(node.children) == 0 {
		return nil, false
	}
	isDir := node.isDir || len(node.children) > 0
	mode := fs.FileMode(0o644)
	size := node.size
	mtime := node.modTime
	if isDir {
		mode = fs.FileMode(os.ModeDir | 0o755)
		size = 0
	}
	if mtime.IsZero() {
		mtime = time.Now()
	}
	base := path.Base(name)
	if name == "." || name == "" {
		base = "."
	}
	return &s3FileInfo{
		name:    base,
		size:    size,
		mode:    mode,
		modTime: mtime,
	}, true
}
