package fs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
	"github.com/pgm/sply2/core"
)

func Mount(dir string, ds *core.DataStore) {
	c, err := fuse.Mount(dir)
	if err != nil {
		panic(err)
	}

	s := New(c, ds)

	for {
		req, err := c.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		fmt.Printf("Req start: %v\n", req)
		rID := req.Hdr().ID
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			err := s.serve(req)
			fmt.Printf("Req done: %v, err=%v\n", req, err)
			s.meta.Lock()
			s.reqs[rID] = nil
			s.meta.Unlock()
		}()
	}

	// wait until all go routines have completed before exiting
	s.wg.Wait()

	// shut it down
	s.ds.Close()
}

type sRequest struct {
	Request fuse.Request
	cancel  func()
}

type handlerPanickedError struct {
	Request interface{}
	Err     interface{}
}

func (h handlerPanickedError) Error() string {
	return fmt.Sprintf("handler panicked: %v", h.Err)
}

// handlerTerminatedError happens when a handler terminates itself
// with runtime.Goexit. This is most commonly because of incorrect use
// of testing.TB.FailNow, typically via t.Fatal.
type handlerTerminatedError struct {
	Request interface{}
}

func (h handlerTerminatedError) Error() string {
	return fmt.Sprintf("handler terminated (called runtime.Goexit)")
}

func (c *Server) debug(msg string) {
	fmt.Println(msg)
}

func (c *Server) serve(r fuse.Request) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &sRequest{Request: r, cancel: cancel}

	// c.debug(request{
	// 	Op:      opName(r),
	// 	Request: r.Hdr(),
	// 	In:      r,
	// })
	// var node Node
	// var snode *serveNode
	c.meta.Lock()
	hdr := r.Hdr()
	if c.reqs[hdr.ID] != nil {
		// This happens with OSXFUSE.  Assume it's okay and
		// that we'll never see an interrupt for this one.
		// Otherwise everything wedges.  TODO: Report to OSXFUSE?
		//
		// TODO this might have been because of missing done() calls
		fmt.Printf("Received request with an ID (%d) which is already live.\n", hdr.ID)
	} else {
		c.reqs[hdr.ID] = req
	}
	c.meta.Unlock()

	// Call this before responding.
	// After responding is too late: we might get another request
	// with the same ID and be very confused.
	// done := func(resp interface{}) {
	// 	msg := response{
	// 		Op:      opName(r),
	// 		Request: logResponseHeader{ID: hdr.ID},
	// 	}
	// 	if err, ok := resp.(error); ok {
	// 		msg.Error = err.Error()
	// 		if ferr, ok := err.(fuse.ErrorNumber); ok {
	// 			errno := ferr.Errno()
	// 			msg.Errno = errno.ErrnoName()
	// 			if errno == err {
	// 				// it's just a fuse.Errno with no extra detail;
	// 				// skip the textual message for log readability
	// 				msg.Error = ""
	// 			}
	// 		} else {
	// 			msg.Errno = fuse.DefaultErrno.ErrnoName()
	// 		}
	// 	} else {
	// 		msg.Out = resp
	// 	}
	// 	c.debug(msg)

	// 	c.meta.Lock()
	// 	delete(c.reqs, hdr.ID)
	// 	c.meta.Unlock()
	// }

	var responded bool
	defer func() {
		if rec := recover(); rec != nil {
			const size = 1 << 16
			buf := make([]byte, size)
			n := runtime.Stack(buf, false)
			buf = buf[:n]
			log.Printf("fuse: panic in handler for %v: %v\n%s", r, rec, buf)
			err := handlerPanickedError{
				Request: r,
				Err:     rec,
			}
			// done(err)
			r.RespondError(err)
			return
		}

		if !responded {
			err := handlerTerminatedError{
				Request: r,
			}
			// done(err)
			r.RespondError(err)
		}
	}()

	err := c.handleRequest(ctx, r)
	if err != nil {
		if err == context.Canceled {
			err = fuse.EINTR
		}
		fuseErr := mapError(err)
		fmt.Printf("mapped %v -> %v\n", err, fuseErr)
		r.RespondError(fuseErr)
	}

	// if err := c.handleRequest(ctx, r); err != nil {
	// 	if err == context.Canceled {
	// 		select {
	// 		case <-parentCtx.Done():
	// 			// We canceled the parent context because of an
	// 			// incoming interrupt request, so return EINTR
	// 			// to trigger the right behavior in the client app.
	// 			//
	// 			// Only do this when it's the parent context that was
	// 			// canceled, not a context controlled by the program
	// 			// using this library, so we don't return EINTR too
	// 			// eagerly -- it might cause busy loops.
	// 			//
	// 			// Decent write-up on role of EINTR:
	// 			// http://250bpm.com/blog:12
	// 			err = fuse.EINTR
	// 		default:
	// 			// nothing
	// 		}
	// 	}
	// 	// done(err)
	// 	r.RespondError(err)
	// }

	// disarm runtime.Goexit protection
	responded = true

	return err
}

func New(c *fuse.Conn, ds *core.DataStore) *Server {
	return &Server{conn: c, ds: ds, reqs: make(map[fuse.RequestID]*sRequest),
		handles:      make(map[fuse.HandleID]*sHandle),
		lastHandleID: 1,
		maxHandles:   100}
}

type Server struct {
	conn *fuse.Conn

	// state, protected by meta
	meta         sync.Mutex
	reqs         map[fuse.RequestID]*sRequest
	handles      map[fuse.HandleID]*sHandle
	lastHandleID int
	maxHandles   int

	// Used to ensure worker goroutines finish before Serve returns
	wg sync.WaitGroup

	ds *core.DataStore
}

type sHandle struct {
	inode    core.INode
	readData []byte
	ref      interface{}
}

func (h *sHandle) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	if h.ref == nil {
		return fuse.EIO
	}

	reader, ok := h.ref.(core.Reader)
	if !ok {
		return fuse.EIO
	}

	_, err := reader.Seek(req.Offset, 0)
	if err != nil {
		return err
	}

	n, err := reader.Read(ctx, res.Data[:req.Size])
	if err != nil {
		return err
	}

	res.Data = res.Data[:n]
	return nil
}

func (h *sHandle) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	if h.ref == nil {
		return fuse.EIO
	}

	writer, ok := h.ref.(core.WritableRef)
	if !ok {
		return fuse.EIO
	}

	_, err := writer.Seek(req.Offset, 0)
	if err != nil {
		return err
	}

	n, err := writer.Write(req.Data)
	if err != nil {
		return err
	}

	res.Size = n
	return nil
}

func (h *sHandle) Release() error {
	return nil
}

// func (c *Server) splitPath(ctx context.Context, fullPath string) (core.INode, string, error) {
// 	var err error

// 	if fullPath == "" {
// 		return core.RootINode, ".", nil
// 	}

// 	components := strings.Split(fullPath[1:], "/")
// 	parent := core.INode(core.RootINode)
// 	for _, component := range components[0 : len(components)-1] {
// 		parent, err = c.ds.GetNodeID(ctx, parent, component)
// 		if err != nil {
// 			return core.InvalidINode, "", err
// 		}
// 	}
// 	return parent, components[len(components)-1], nil
// }

// func (c *Server) getINode(ctx context.Context, relPath string) (core.INode, error) {
// 	parent, name, err := c.splitPath(ctx, relPath)
// 	if err != nil {
// 		return core.InvalidINode, err
// 	}
// 	inode, err := c.ds.GetNodeID(ctx, parent, name)
// 	if err != nil {
// 		return core.InvalidINode, err
// 	}
// 	return inode, nil
// }

func (c *Server) handleRequest(ctx context.Context, r fuse.Request) error {
	switch r := r.(type) {
	default:
		// Note: To FUSE, ENOSYS means "this server never implements this request."
		// It would be inappropriate to return ENOSYS for other operations in this
		// switch that might only be unavailable in some contexts, not all.
		return fuse.ENOSYS

	// Node operations.
	case *fuse.GetattrRequest:
		s := &fuse.GetattrResponse{}
		err := c.Getattr(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	// case *fuse.SetattrRequest:
	// 	s := &fuse.SetattrResponse{}
	// 	err := c.Setattr(r, s)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	r.Respond(s)
	// 	return nil

	case *fuse.RemoveRequest:
		err := c.Remove(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.AccessRequest:
		err := c.Access(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.LookupRequest:
		s := &fuse.LookupResponse{}
		err := c.Lookup(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.MkdirRequest:
		s := &fuse.MkdirResponse{}
		err := c.Mkdir(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.OpenRequest:
		s := &fuse.OpenResponse{}
		err := c.Open(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.CreateRequest:
		s := &fuse.CreateResponse{OpenResponse: fuse.OpenResponse{}}
		err := c.Create(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.ForgetRequest:
		err := c.Forget(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.RenameRequest:
		err := c.Rename(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	// Handle operations.
	case *fuse.ReadRequest:
		s := &fuse.ReadResponse{Data: make([]byte, 0, r.Size)}
		err := c.Read(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.WriteRequest:
		s := &fuse.WriteResponse{}
		err := c.Write(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.FlushRequest:
		err := c.Flush(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.ReleaseRequest:
		err := c.Release(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	// filesystem operations
	case *fuse.StatfsRequest:
		s := &fuse.StatfsResponse{}
		err := c.Statfs(ctx, r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.DestroyRequest:
		err := c.Destroy(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.FsyncRequest:
		err := c.Fsync(ctx, r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.InterruptRequest:
		err := c.Interrupt(r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil
	}

	panic("not reached")
}

func (c *Server) Getattr(ctx context.Context, req *fuse.GetattrRequest, res *fuse.GetattrResponse) error {
	err := c.getattr(ctx, core.INode(req.Node), &res.Attr)
	if err != nil {
		return err
	}
	return nil
}

func (c *Server) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	err := c.ds.Remove(ctx, core.INode(req.Node), req.Name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Server) Access(ctx context.Context, r *fuse.AccessRequest) error {
	return nil
}

func (c *Server) getattr(ctx context.Context, inode core.INode, attr *fuse.Attr) error {
	nattr, err := c.ds.GetAttr(ctx, inode)
	if err != nil {
		return err
	}

	attr.Valid = 0 * time.Second
	attr.Inode = uint64(inode)
	attr.Size = uint64(attr.Size)           // size in bytes
	attr.Blocks = uint64(attr.Size/512 + 1) // size in 512-byte units
	attr.Atime = nattr.ModTime              // time of last access
	attr.Mtime = nattr.ModTime              // time of last modification
	attr.Ctime = nattr.ModTime              // time of last inode change
	attr.Crtime = nattr.ModTime             // time of creation (OS X only)
	if nattr.IsDir {
		attr.Mode = 0777 | os.ModeDir // file mode
	} else {
		attr.Mode = 0777 // file mode
	}
	attr.Nlink = 1 // number of links (usually 1)
	// resp.Attr.Uid = 0      // owner uid
	// resp.Attr.Gid = 0     // group gid
	// resp.Attr.Rdev = 0     // device numbers
	// resp.Attr.Flags     uint32      // chflags(2) flags (OS X only)
	attr.BlockSize = 4 * 1024 // preferred blocksize for filesystem I/O. I don't know the implication of setting this

	return nil
}

func mapError(err error) error {
	if err == core.NoSuchNodeErr {
		return fuse.ENOENT
	}

	return fuse.EIO
}

func (c *Server) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) error {
	inode, err := c.ds.GetNodeID(ctx, core.INode(req.Node), req.Name)
	if err != nil {
		return err
	}

	err = c.getattr(ctx, inode, &resp.Attr)
	if err != nil {
		return err
	}

	// TODO: Not sure about these
	resp.EntryValid = 0
	resp.Generation = 0
	resp.Node = fuse.NodeID(inode)

	return nil
}

func (c *Server) Mkdir(ctx context.Context, req *fuse.MkdirRequest, res *fuse.MkdirResponse) error {
	// parent, name, err := c.splitPath(ctx, req.Name)
	// if err != nil {
	// 	return err
	// }

	inode, err := c.ds.MakeDir(ctx, core.INode(req.Node), req.Name)
	if err != nil {
		return err
	}

	err = c.getattr(ctx, inode, &res.Attr)
	if err != nil {
		return err
	}

	// TODO: Not sure about these
	res.EntryValid = 0
	res.Generation = 0
	res.Node = fuse.NodeID(inode)

	return nil
}

func (c *Server) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) error {
	if req.Dir {
		c.meta.Lock()
		hID := c.lastHandleID
		var h *sHandle
		for {
			hID++
			if hID > c.maxHandles {
				hID = 1
			}
			if hID == c.lastHandleID {
				// we've wrapped around
				panic("Exhausted max handles")
			}
			h = c.handles[fuse.HandleID(hID)]
			if h == nil {
				break
			}
		}
		c.lastHandleID = hID
		c.handles[fuse.HandleID(hID)] = &sHandle{inode: core.INode(req.Node)}
		c.meta.Unlock()

		res.Handle = fuse.HandleID(hID)
		return nil
	}
	// opening a regular file

	return fuse.EPERM
	// return nil
}

func (c *Server) bindHandle(obj interface{}) fuse.HandleID {
	newSHandle := &sHandle{ref: obj}

	c.meta.Lock()
	defer c.meta.Unlock()
	handle := c.lastHandleID + 1
	firstTryHandle := handle
	for {
		existing := c.handles[fuse.HandleID(c.lastHandleID)]
		if existing == nil {
			break
		}
		c.lastHandleID++
		if firstTryHandle == c.lastHandleID {
			panic("# of file handles exhausted")
		}
	}

	c.lastHandleID = handle

	c.handles[fuse.HandleID(handle)] = newSHandle
	return fuse.HandleID(handle)
}

func (c *Server) Create(ctx context.Context, req *fuse.CreateRequest, res *fuse.CreateResponse) error {
	log.Printf("Create called")
	inode, ref, err := c.ds.CreateWritable(ctx, core.INode(req.Node), req.Name)
	if err != nil {
		log.Printf("Create failed: %v", err)
		return err
	}
	err = c.getattr(ctx, inode, &res.LookupResponse.Attr)
	handle := c.bindHandle(ref)
	res.OpenResponse.Handle = handle
	return nil
}

// forget a node
func (c *Server) Forget(ctx context.Context, req *fuse.ForgetRequest) error {
	return nil
}

func (c *Server) Rename(ctx context.Context, req *fuse.RenameRequest) error {
	err := c.ds.Rename(ctx, core.INode(req.Node), req.OldName, core.INode(req.NewDir), req.NewName)
	if err != nil {
		return err
	}
	return nil
}

func (c *Server) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	c.meta.Lock()
	h, handleValid := c.handles[req.Handle]
	c.meta.Unlock()

	if !handleValid {
		return fuse.ESTALE
	}

	if req.Dir {
		// detect rewinddir(3) or similar seek and refresh
		// contents
		if req.Offset == 0 {
			h.readData = nil
		}

		if h.readData == nil {
			dirs, err := c.ds.GetDirContents(ctx, h.inode)
			if err != nil {
				return err
			}

			fmt.Printf("dirs=%v\n", dirs)

			var data []byte
			for _, dir := range dirs {
				entryType := fuse.DT_File
				if dir.IsDir {
					entryType = fuse.DT_Dir
				}
				data = fuse.AppendDirent(data, fuse.Dirent{Inode: uint64(dir.ID), Type: entryType, Name: dir.Name})
			}

			h.readData = data
		}
		fuseutil.HandleRead(req, res, h.readData)
		return nil
	} else {
		if err := h.Read(ctx, req, res); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	c.meta.Lock()
	h, handleValid := c.handles[req.Handle]
	c.meta.Unlock()

	if !handleValid {
		return fuse.ESTALE
	}

	if err := h.Write(ctx, req, res); err != nil {
		return err
	}

	return nil
}

func (c *Server) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return nil
}

func (c *Server) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	c.meta.Lock()
	h, handleValid := c.handles[req.Handle]
	if handleValid {
		delete(c.handles, req.Handle)
	}
	c.meta.Unlock()

	if !handleValid {
		return fuse.ESTALE
	}

	h.Release()

	return nil
}

func (c *Server) Statfs(ctx context.Context, req *fuse.StatfsRequest, res *fuse.StatfsResponse) error {
	// TODO: Review these
	res.Blocks = 1000000
	res.Bfree = 1000000
	res.Bavail = 1000000
	res.Files = 1000000
	res.Ffree = 1000000
	res.Bsize = 1024
	res.Namelen = 4 * 1024
	res.Ffree = 1
	return nil
}

func (c *Server) Destroy(ctx context.Context, req *fuse.DestroyRequest) error {
	return nil
}

func (c *Server) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (c *Server) Interrupt(r *fuse.InterruptRequest) error {
	c.meta.Lock()
	ireq := c.reqs[r.IntrID]
	if ireq != nil && ireq.cancel != nil {
		ireq.cancel()
		ireq.cancel = nil
	}
	c.meta.Unlock()

	return nil
}
