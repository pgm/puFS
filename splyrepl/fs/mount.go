package fs

import (
	"os"

	"bazil.org/fuse"
)

func main() {
	dir := os.Args[1]
	c, err := fuse.Mount(dir)
	if err != nil {
		panic(err)
	}

	for {
		req, err := c.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		
	}
}

type Server struct {
	// state, protected by meta
	meta       sync.Mutex
	reqs        map[fuse.RequestID]*serveRequest
	handles     map[fuse.HandleID]*serveHandle

	// Used to ensure worker goroutines finish before Serve returns
	wg sync.WaitGroup
}

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
		err := c.Getattr(r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.SetattrRequest:
		s := &fuse.SetattrResponse{}
		err := c.Setattr(r, s)
		if err != nil {
			return err
		}
		r.Respond(s)
		return nil

	case *fuse.RemoveRequest:
		err := c.Remove(r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.AccessRequest:
		err := c.Access(r)
		if err != nil {
			return err
		}
		r.Respond()
		return nil

	case *fuse.LookupRequest:
		s := &fuse.LookupResponse{}
		err := c.Lookup(r, s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.MkdirRequest:
		s := &fuse.MkdirResponse{}
		err := c.Mkdir(r, s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.OpenRequest:
		s := &fuse.OpenResponse{}
		err := c.Open(r, s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.CreateRequest:
		s := &fuse.CreateResponse{OpenResponse: fuse.OpenResponse{}}
		err := c.Create(r, s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.ForgetRequest:
		err := c.Forget(r)
		if err != nil {
			return err
		}		
		r.Respond()
		return nil

	case *fuse.RenameRequest:
		err := c.Rename(r)
		if err != nil {
			return err
		}		
		r.Respond()
		return nil
		
	// Handle operations.
	case *fuse.ReadRequest:
		s := &fuse.ReadResponse{Data: make([]byte, 0, r.Size)}
		err := c.Read(r, s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.WriteRequest:
		s := &fuse.WriteResponse{}
		err := c.Write(r,s)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.FlushRequest:
		err := c.Flush(r)
		if err != nil {
			return err
		}		
		r.Respond()
		return nil

	case *fuse.ReleaseRequest:
		err := c.Release(r)
		if err != nil {
			return err
		}		
		r.Respond()
		return nil

	// filesystem operations
	case *fuse.StatfsRequest:
		s := &fuse.StatfsResponse{}
		err := c.Statfs(r)
		if err != nil {
			return err
		}		
		r.Respond(s)
		return nil

	case *fuse.DestroyRequest:
		err := c.Destroy(r)
		if err != nil {
			return err
		}		
		r.Respond()
		return nil

	case *fuse.FsyncRequest:
		err := c.Fsync(r)
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

func Interrupt() error {
	c.meta.Lock()
	ireq := c.req[r.IntrID]
	if ireq != nil && ireq.cancel != nil {
		ireq.cancel()
		ireq.cancel = nil
	}
	c.meta.Unlock()

	return nil
}
