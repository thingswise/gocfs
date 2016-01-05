// gocfs
/*
Copyright 2015-2016 Thingswise, LLC and contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/gocql/gocql"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
)

var log = logging.MustGetLogger("gocfs")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

var ro = flag.Bool("ro", false, "mount readonly")
var cassandra = flag.String("db", "localhost", "cassandra endpoint")
var debug = flag.Bool("v", false, "verbose output")
var daemon = flag.Bool("d", false, "run in background")
var consistency = flag.String("c", "quorum", "cassandra consistency level (r/w)")

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var lock = &sync.Mutex{}
var inodeLock = &sync.Mutex{}

func main() {
	flag.Usage = Usage
	flag.Parse()

	logging.SetFormatter(format)
	if *debug {
		logging.SetLevel(logging.DEBUG, "gocfs")
	} else {
		logging.SetLevel(logging.ERROR, "gocfs")
	}

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	if *daemon {
		var args = make([]string, 0)
		if *ro {
			args = append(args, "-ro")
		}
		args = append(args, "-db", *cassandra)
		if *debug {
			args = append(args, "-v")
		}
		args = append(args, flag.Args()...)
		cmd := exec.Command(os.Args[0], args...)
		cmd.Start()
		return
	}

	var mountOptions = make([]fuse.MountOption, 0)

	mountOptions = append(mountOptions, fuse.FSName("gocfs"))
	mountOptions = append(mountOptions, fuse.Subtype("gocfs"))
	mountOptions = append(mountOptions, fuse.VolumeName("cassandra"))
	//mountOptions = append(mountOptions, fuse.UseIno())
	//mountOptions = append(mountOptions, fuse.WritebackCache())
	//mountOptions = append(mountOptions, fuse.MaxReadahead(0))
	if *ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}

	c, err := fuse.Mount(
		mountpoint,
		mountOptions...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var consistencyLevel gocql.Consistency
	if *consistency == "quorum" {
		consistencyLevel = gocql.Quorum
	} else if *consistency == "one" {
		consistencyLevel = gocql.One
	} else if *consistency == "all" {
		consistencyLevel = gocql.All
	} else {
		log.Fatal("Unsupported consistency level: %s", *consistency)
	}

	cluster := gocql.NewCluster(*cassandra)
	cluster.DiscoverHosts = true
	cluster.Timeout = 2 * time.Second
	cluster.Consistency = consistencyLevel

	session, _ := cluster.CreateSession()
	defer session.Close()

	if err := initStorage(session); err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, &fs.Config{
		Debug: debugFs,
	})

	err = server.Serve(FS{
		Session:   session,
		Server:    server,
		Conn:      c,
		blockSize: 65536,
		id2path:   make(map[uint64]string),
		path2id:   make(map[string]uint64),
		nextId:    0,
	})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

func debugFs(msg interface{}) {
	log.Debug("FUSE: %s", msg)
}

func initStorage(session *gocql.Session) error {
	if err := session.Query("CREATE KEYSPACE IF NOT EXISTS fuse WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };").Exec(); err != nil {
		return err
	}
	if err := session.Query(`CREATE TABLE IF NOT EXISTS fuse.filesystem (
          filename text,
          entry    text,
          created  bigint,
          modified bigint,
          access   bigint,
          block    int,
          data     blob,
          is_dir   boolean,
          PRIMARY KEY(filename, block, entry));`).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

// FS implements the hello world file system.
type FS struct {
	Session   *gocql.Session
	Server    *fs.Server
	Conn      *fuse.Conn
	blockSize uint64

	id2path map[uint64]string
	path2id map[string]uint64
	nextId  uint64
}

var pathLock = &sync.Mutex{}

func (fs *FS) GetPathID(path string) uint64 {
	pathLock.Lock()
	defer pathLock.Unlock()

	var id uint64
	id, ok := fs.path2id[path]
	if !ok {
		id = fs.nextId
		fs.nextId++
		fs.path2id[path] = id
		fs.id2path[id] = path
	}

	return id
}

func (fs *FS) GetPath(pathId uint64) string {
	pathLock.Lock()
	defer pathLock.Unlock()

	path, ok := fs.id2path[pathId]
	if !ok {
		log.Fatal("Cannot find path for id=%d", pathId)
		return ""
	} else {
		return path
	}
}

func (fs *FS) DropPath(path string) {
	pathLock.Lock()
	defer pathLock.Unlock()

	pathId, ok := fs.path2id[path]
	if ok {
		delete(fs.path2id, path)
		delete(fs.id2path, pathId)
	}
}

func (fs *FS) ReplacePath(oldPath string, newPath string) {
	pathLock.Lock()
	defer pathLock.Unlock()

	pathId, ok := fs.path2id[oldPath]
	if ok {
		delete(fs.path2id, oldPath)
		fs.path2id[newPath] = pathId
		fs.id2path[pathId] = newPath
	}
}

func (_fs FS) Root() (fs.Node, error) {
	return Dir{PathId: _fs.GetPathID("/"), Fs: &_fs}, nil
}

func (fs FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	resp.Blocks = 18446744073709551615
	resp.Bfree = 18446744073709551615  // Free blocks in file system.
	resp.Bavail = 18446744073709551615 // Free blocks in file system if you're not root.
	resp.Files = 0                     // Total files in file system.
	resp.Ffree = 18446744073709551615  // Free files in file system.
	resp.Bsize = uint32(fs.blockSize)  // Block size
	resp.Namelen = 4294967295          // Maximum file name length?
	resp.Frsize = 1                    // Fragment size, smallest addressable data size in the file system.
	return nil
}

func (_fs FS) GenerateInode(parentInode uint64, name string) uint64 {
	inode := fs.GenerateDynamicInode(parentInode, name)
	log.Debug("GenerateInode(%d,%s)=%d", parentInode, name, inode)
	return inode
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	Fs     *FS
	Parent *Dir
	//Path   string
	PathId uint64
}

func (d Dir) GetPath() string {
	return d.Fs.GetPath(d.PathId)
}

type BasicAttr struct {
	Created  uint64
	Modified uint64
	Access   uint64
	IsDir    bool
}

func basicAttr(fs *FS, path string) (*BasicAttr, error) {
	iter := fs.Session.Query("SELECT created,modified,access,is_dir FROM fuse.filesystem WHERE filename=? AND entry='!' AND block=0;", path).Iter()
	var created uint64
	var modified uint64
	var access uint64
	var is_dir bool
	for iter.Scan(&created, &modified, &access, &is_dir) {
		return &BasicAttr{Created: created, Modified: modified, Access: access, IsDir: is_dir}, nil
	}
	return nil, fuse.ENOENT
}

func (d Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// no need to lock since no cassandra access
	log.Debug("Dir.Attr(%s)", d.GetPath())
	a.Valid = time.Second
	a.Inode = 0
	a.Mode = os.ModeDir | os.ModePerm
	return nil
}

func (d Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, res *fuse.LookupResponse) (fs.Node, error) {
	log.Debug("Lookup(%s,%d,%s)", d.GetPath(), req.Node, req.Name)
	lock.Lock()
	defer lock.Unlock()

	node, err := d.lookup(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	res.Node = fuse.NodeID(d.Fs.GenerateInode(uint64(req.Node), req.Name))
	return node, nil
}

func (d Dir) lookup(ctx context.Context, name string) (fs.Node, error) {
	iter := d.Fs.Session.Query("SELECT entry FROM fuse.filesystem WHERE filename=? AND block=0 AND entry=?;", d.GetPath(), name).Iter()
	var entry string
	p := path.Join(d.GetPath(), name)
	for iter.Scan(&entry) {
		attr, err := basicAttr(d.Fs, p)
		if err != nil {
			log.Debug(err.Error())
			return nil, err
		} else {
			var e fs.Node
			if attr.IsDir {
				e = Dir{Fs: d.Fs, Parent: &d, PathId: d.Fs.GetPathID(p)}
			} else {
				e = File{Fs: d.Fs, Parent: &d, PathId: d.Fs.GetPathID(p)}
			}
			return e, nil
		}
	}

	log.Debug("Not found: %s", p)
	return nil, fuse.ENOENT
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("ReadDirAll(%s)", d.GetPath())
	iter := d.Fs.Session.Query("SELECT entry FROM fuse.filesystem WHERE filename=? AND block=0 AND entry > '!' ALLOW FILTERING;", d.GetPath()).Iter()
	var result []fuse.Dirent = make([]fuse.Dirent, 0)
	var entry string
	for iter.Scan(&entry) {
		log.Debug("> cassandra entry=%s", entry)
		result = append(result, fuse.Dirent{Type: fuse.DT_Unknown, Name: entry})
	}
	return result, nil
}

func (d Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Mkdir(%s,%s)", d.GetPath(), req.Name)
	p := path.Join(d.GetPath(), req.Name)
	created := time.Now().Unix()
	modified := created
	access := created
	var err = d.Fs.Session.Query("INSERT INTO fuse.filesystem (filename, entry, created, modified, access, block, data, is_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", d.GetPath(), req.Name, 0, 0, 0, 0, make([]byte, 0), false).Exec()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	err = d.Fs.Session.Query("INSERT INTO fuse.filesystem (filename, entry, created, modified, access, block, data, is_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", p, "!", created, modified, access, 0, make([]byte, 0), true).Exec()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return Dir{PathId: d.Fs.GetPathID(p), Parent: &d, Fs: d.Fs}, nil
}

func (d Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Create(%s,%s)", d.GetPath(), req.Name)
	p := path.Join(d.GetPath(), req.Name)
	created := time.Now().Unix()
	modified := created
	access := created
	var err = d.Fs.Session.Query("INSERT INTO fuse.filesystem (filename, entry, created, modified, access, block, data, is_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", d.GetPath(), req.Name, 0, 0, 0, 0, make([]byte, 0), false).Exec()
	if err != nil {
		log.Error(err.Error())
		return nil, nil, err
	}
	err = d.Fs.Session.Query("INSERT INTO fuse.filesystem (filename, entry, created, modified, access, block, data, is_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", p, "!", created, modified, access, 0, make([]byte, 0), false).Exec()
	if err != nil {
		log.Error(err.Error())
		return nil, nil, err
	}
	file := File{PathId: d.Fs.GetPathID(p), Parent: &d, Fs: d.Fs}
	var mode os.FileMode = 0
	resp.LookupResponse.Attr = fuse.Attr{Valid: time.Second, Inode: 0, Size: 0, Atime: time.Unix(int64(access), 0), Mtime: time.Unix(int64(modified), 0), Ctime: time.Unix(int64(created), 0), Mode: mode}
	return file, file, nil
}

func (d Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Remove(%s,%s)", d.GetPath(), req.Name)
	p := path.Join(d.GetPath(), req.Name)
	iter := d.Fs.Session.Query("SELECT entry FROM fuse.filesystem WHERE filename=? AND block=0 AND entry > '!';", p).Iter()
	var entry string
	for iter.Scan(&entry) {
		return fuse.Errno(syscall.ENOTEMPTY)
	}
	if err := d.Fs.Session.Query("DELETE FROM fuse.filesystem WHERE filename=? AND entry=? AND block=0;", d.GetPath(), req.Name).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}
	if err := d.Fs.Session.Query("DELETE FROM fuse.filesystem WHERE filename=?;", p).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}
	d.Fs.DropPath(p)
	return nil
}

func (d Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

// File implements both Node and Handle
type File struct {
	Fs     *FS
	Parent *Dir
	PathId uint64
}

func (f File) GetPath() string {
	return f.Fs.GetPath(f.PathId)
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Attr(%s,%d)", f.GetPath(), f.PathId)
	a.Valid = time.Second
	a.Inode = 0
	a.Mode = os.ModePerm
	var err error
	a.Size, err = f.getSize()
	if err != nil {
		log.Error("getSize error: %s", err.Error())
		return err
	}
	if attr, err := basicAttr(f.Fs, f.GetPath()); err != nil {
		log.Error("basicAttr error: %s", err.Error())
		return err
	} else {
		a.Mtime = time.Unix(int64(attr.Modified), 0)
		a.Ctime = time.Unix(int64(attr.Created), 0)
		a.Atime = time.Unix(int64(attr.Access), 0)
	}
	log.Debug("size=%d", a.Size)
	return nil
}

func (f File) getSize() (uint64, error) {
	blocks, err := f.getBlocks()
	if err != nil {
		return 0, err
	}
	if blocks > 0 {
		block, err := f.getBlock(blocks)
		if err != nil {
			return 0, err
		}
		log.Debug("lastBlockSize=%d", len(block))
		return f.Fs.blockSize*uint64(blocks-1) + uint64(len(block)), nil
	} else {
		return 0, nil
	}
}

func (f File) getBlocks() (int, error) {
	iter := f.Fs.Session.Query("SELECT block FROM fuse.filesystem WHERE filename=? AND block > 0 ORDER BY block DESC LIMIT 1;", f.GetPath()).Iter()
	var blocks int
	for iter.Scan(&blocks) {
		log.Debug("max block=%d", blocks)
		return blocks, nil
	}
	log.Debug("no blocks")
	return 0, nil
}

func (f File) getBlock(block int) ([]byte, error) {
	iter := f.Fs.Session.Query("SELECT data FROM fuse.filesystem WHERE filename=? AND entry = '!' AND block = ?;", f.GetPath(), block).Iter()
	var data []byte
	for iter.Scan(&data) {
		return data, nil
	}
	log.Debug("getBlock(%d): no such file: %s", block, f.GetPath())
	return nil, fuse.ENOENT
}

func (f File) setBlock(block int, data []byte) error {
	err := f.Fs.Session.Query("INSERT INTO fuse.filesystem (filename, entry, created, modified, access, block, data, is_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", f.GetPath(), "!", 0, 0, 0, block, data, false).Exec()
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

func (f File) set_mtime() error {
	var resp = fuse.SetattrResponse{}
	return f.setattr(nil, &fuse.SetattrRequest{Valid: fuse.SetattrMtimeNow}, &resp)
}

func (f File) doTruncate(length uint64) error {
	log.Debug("doTruncate(%s,%d)", f.GetPath(), length)
	b := length / f.Fs.blockSize
	r := length % f.Fs.blockSize
	if r > 0 {
		b += 1
	}
	blocks, err := f.getBlocks()
	if err != nil {
		return err
	}

	log.Debug(">> b=%d r=%d blocks=%d", b, r, blocks)

	if b < uint64(blocks) {
		log.Debug(">> delete all blocks >= %d", b+1)
		iter := f.Fs.Session.Query("SELECT block FROM fuse.filesystem WHERE filename=? AND block >= ?;", f.GetPath(), b+1).Iter()
		var block int
		for iter.Scan(&block) {
			if err := f.Fs.Session.Query("DELETE FROM fuse.filesystem WHERE filename=? AND entry='!' AND block=?;", f.GetPath(), block).Exec(); err != nil {
				log.Error(err.Error())
				return err
			}
		}
		if r > 0 {
			block, err := f.getBlock(int(b))
			if err != nil {
				return err
			}
			log.Debug(">> 1.truncate block %d to %d", b, r)
			if f.setBlock(int(b), block[:r]) != nil {
				return err
			}
		}
	} else if b == uint64(blocks) {
		if r > 0 {
			blockData, err := f.getBlock(int(b))
			if err != nil {
				return err
			}
			if len(blockData) > int(r) {
				log.Debug(">> 2.truncate block %d to %d", b, r)
				if err := f.setBlock(int(b), blockData[:r]); err != nil {
					return err
				}
			} else if len(blockData) < int(r) {
				log.Debug(">> 1.extend block %d to %d", b, r)
				if err := f.setBlock(int(b), append(blockData, make([]byte, int(r)-len(blockData))...)); err != nil {
					return err
				}
			}
		}
	} else {
		if blocks > 0 {
			block, err := f.getBlock(blocks)
			if err != nil {
				return err
			}
			if len(block) < int(f.Fs.blockSize) {
				log.Debug(">> 2.extend block %d to %d", blocks, f.Fs.blockSize)
				err := f.setBlock(blocks, append(block, make([]byte, int(f.Fs.blockSize)-len(block))...))
				if err != nil {
					return err
				}
			}
		}
		var i = blocks + 1
		for i < int(b) {
			log.Debug(">> 2.fill block %d", i)
			err := f.setBlock(i, make([]byte, f.Fs.blockSize))
			if err != nil {
				return err
			}
			i += 1
		}
		if r == 0 {
			log.Debug(">> 3.fill block %d", b)
			err := f.setBlock(int(b), make([]byte, f.Fs.blockSize))
			if err != nil {
				return err
			}
		} else {
			log.Debug(">> 4.fill block %d to %d", b, r)
			err := f.setBlock(int(b), make([]byte, r))
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (f File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	log.Debug("Setattr(%s,valid=%v,size=%v)", f.GetPath(), req.Valid, req.Size)
	lock.Lock()
	defer lock.Unlock()
	return f.setattr(ctx, req, resp)
}

func (f File) setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	iter := f.Fs.Session.Query("SELECT created, modified, access, is_dir FROM fuse.filesystem WHERE filename=? AND entry='!' AND block=0;").Iter()
	var created uint64 = uint64(time.Now().Unix())
	var modified uint64 = created
	var access uint64 = created
	var is_dir bool = false
	var updated = false
	for iter.Scan(&created, &modified, &access, &is_dir) {
		if req.Valid&fuse.SetattrAtime != 0 {
			access = uint64(req.Atime.Unix())
		}
		if req.Valid&fuse.SetattrMtime != 0 {
			modified = uint64(req.Mtime.Unix())
		}
		if req.Valid&fuse.SetattrAtimeNow != 0 {
			access = uint64(time.Now().Unix())
		}
		if req.Valid&fuse.SetattrMtimeNow != 0 {
			modified = uint64(time.Now().Unix())
		}
		err := f.Fs.Session.Query("INSERT INTO fuse.filesystem(filename, entry, created, modified, access, block, data, is_dir) VALUES(?, ?, ?, ?, ?, ?, ?, ?);", f.GetPath(), "!", created, modified, access, 0, make([]byte, 0), is_dir).Exec()
		if err != nil {
			log.Error(err.Error())
			return err
		}
		updated = true
		break
	}
	if !updated {
		err := f.Fs.Session.Query("INSERT INTO fuse.filesystem(filename, entry, created, modified, access, block, data, is_dir) VALUES(?, ?, ?, ?, ?, ?, ?, ?);", f.GetPath(), "!", created, modified, access, 0, make([]byte, 0), is_dir).Exec()
		if err != nil {
			log.Error(err.Error())
			return err
		}
	}
	var size = req.Size
	if req.Valid&fuse.SetattrSize != 0 {
		err := f.doTruncate(size)
		if err != nil {
			log.Error(err.Error())
			return err
		}
	} else {
		var err error
		size, err = f.getSize()
		if err != nil {
			return err
		}
	}

	var mode os.FileMode
	if is_dir {
		mode = os.ModeDir
	} else {
		mode = 0
	}
	resp.Attr = fuse.Attr{Valid: time.Second, Inode: 0, Size: size, Atime: time.Unix(int64(access), 0), Mtime: time.Unix(int64(modified), 0), Ctime: time.Unix(int64(created), 0), Mode: mode}

	return nil
}

func (f File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Write(%s,%d)", f.GetPath(), len(req.Data))
	data := req.Data

	bo := uint64(req.Offset) / f.Fs.blockSize
	ro := uint64(req.Offset) % f.Fs.blockSize
	var written uint64 = 0
	var toWriteInTheFirstBlock = f.Fs.blockSize - ro
	if toWriteInTheFirstBlock > uint64(len(data)) {
		toWriteInTheFirstBlock = uint64(len(data))
	}
	if ro > 0 {
		block, err := f.getBlock(int(bo + 1))
		if err != nil {
			return err
		}
		log.Debug(">> write-chunk.1:%d[%d:%d]", bo+1, 0, toWriteInTheFirstBlock)
		if err := f.setBlock(int(bo+1), append(block[0:ro], data[:toWriteInTheFirstBlock]...)); err != nil {
			return err
		}
		bo = bo + 1
		written = uint64(toWriteInTheFirstBlock)
	}
	var i = 0
	for len(data)-int(written)-int(f.Fs.blockSize) >= int(f.Fs.blockSize) {
		log.Debug(">> write-chunk.2:%d[%d:%d]; rem=%d", int(bo)+1+i, written, written+f.Fs.blockSize, uint64(len(data))-written)
		err := f.setBlock(int(bo)+1+i, data[written:written+f.Fs.blockSize])
		if err != nil {
			return err
		}
		written += f.Fs.blockSize
		i += 1
	}
	if len(data)-int(written) > 0 {
		log.Debug(">> write-chunk.3:%d[%d:%d]", int(bo)+1+i, written, len(data))
		err := f.setBlock(int(bo)+1+i, data[written:])
		if err != nil {
			return err
		}
		written = uint64(len(data))
	}

	err := f.doTruncate(uint64(req.Offset) + uint64(len(req.Data)))
	if err != nil {
		log.Error(err.Error())
		return err
	}

	resp.Size = int(written)
	return f.set_mtime()

}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	lock.Lock()
	defer lock.Unlock()
	log.Debug("Read(%s,size=%v,offset=%v)", f.GetPath(), req.Size, req.Offset)
	offset := req.Offset
	size := req.Size
	bfrom := offset / int64(f.Fs.blockSize)                  // int64
	rfrom := offset % int64(f.Fs.blockSize)                  // int64
	var rto = (offset + int64(size)) % int64(f.Fs.blockSize) // int64
	bto := (offset + int64(size)) / int64(f.Fs.blockSize)

	if rto > 0 {
		bto += 1
	}

	log.Debug("bfrom=%d rfrom=%d bto=%d rto=%d", bfrom, rfrom, bto, rto)
	iter := f.Fs.Session.Query("SELECT data FROM fuse.filesystem WHERE filename=? AND block >= ? AND block < ?;", f.GetPath(), bfrom+1, bto+1).Iter()
	var result []byte = make([]byte, 0)
	var prevbuf []byte = nil
	var data []byte
	var i int
	for i = 0; iter.Scan(&data); i++ {
		if prevbuf != nil {
			if i == 1 {
				log.Debug("appending prevbuf(1)=%d", len(prevbuf)-int(rfrom))
				result = append(result, prevbuf[rfrom:]...)
			} else {
				log.Debug("appending prevbuf(2)=%d", len(prevbuf))
				result = append(result, prevbuf...)
			}
		}
		prevbuf = data
	}
	if rto == 0 {
		rto = int64(f.Fs.blockSize)
	}

	if bfrom+int64(i) == bto {
		// all blocks read
		if i == 1 {
			log.Debug("appending prevbuf(3)=%d", min(len(prevbuf), int(rto))-int(rfrom))
			result = append(result, prevbuf[int(rfrom):min(len(prevbuf), int(rto))]...)
		} else {
			log.Debug("appending prevbuf(4)=%d", min(len(prevbuf), int(rto)))
			result = append(result, prevbuf[:min(len(prevbuf), int(rto))]...)
		}
	} else {
		// read fewer blocks than requested
		if i == 1 {
			log.Debug("appending prevbuf(5)=%d", len(prevbuf)-int(rfrom))
			result = append(result, prevbuf[int(rfrom):]...)
		} else {
			log.Debug("appending prevbuf(6)=%d", len(prevbuf))
			result = append(result, prevbuf...)
		}
	}

	resp.Data = result
	log.Debug("resp.Data=%d", len(resp.Data))
	return nil
}

func (f File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (d Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	lock.Lock()
	defer lock.Unlock()

	log.Debug("Rename(dir=%s,%s,%s)", d.GetPath(), req.OldName, req.NewName)

	oldPath := path.Join(d.GetPath(), req.OldName)

	if ba, err := basicAttr(d.Fs, oldPath); err != nil {
		return err
	} else {
		if ba.IsDir {
			return fuse.ENOSYS
		}
	}

	dst, _ := newDir.(Dir)

	if err := d.Fs.Session.Query("DELETE FROM fuse.filesystem WHERE filename=? AND entry=? AND block=0;", d.GetPath(), req.OldName).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}

	iter := d.Fs.Session.Query("SELECT entry, created, modified, access, block, data, is_dir FROM fuse.filesystem WHERE filename=?;", path.Join(d.GetPath(), req.OldName)).Iter()

	var entry string
	var created uint64
	var modified uint64
	var access uint64
	var block int
	var data []byte
	var is_dir bool

	for iter.Scan(&entry, &created, &modified, &access, &block, &data, &is_dir) {
		if err := d.Fs.Session.Query("INSERT INTO fuse.filesystem(filename,entry, created, modified, access, block, data, is_dir) VALUES (?,?,?,?,?,?,?,?);", path.Join(dst.GetPath(), req.NewName), entry, created, modified, access, block, data, is_dir).Exec(); err != nil {
			log.Error(err.Error())
			return err
		}
	}

	if err := d.Fs.Session.Query("DELETE FROM fuse.filesystem WHERE filename=?;", path.Join(d.GetPath(), req.OldName)).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}

	if err := d.Fs.Session.Query("INSERT INTO fuse.filesystem(filename,entry, created, modified, access, block, data, is_dir) VALUES (?,?,?,?,?,?,?,?);", dst.GetPath(), req.NewName, 0, 0, 0, 0, make([]byte, 0), false).Exec(); err != nil {
		log.Error(err.Error())
		return err
	}

	newPath := path.Join(dst.GetPath(), req.NewName)
	d.Fs.ReplacePath(oldPath, newPath)

	return nil
}
