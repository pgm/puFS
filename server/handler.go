package api
import (
  "log"
  "golang.org/x/net/context"
)
// Server represents the gRPC server
type Server struct {
}

// SayHello generates response to a Ping request
func (s *Server) GetDirContents(ctx context.Context, in *DirContentsRequest) (*DirContentsResponse, error) {
  log.Printf("Receive message %s", in.Greeting)
  return &DirContentsResponse{Greeting: "bar"}, nil
}
