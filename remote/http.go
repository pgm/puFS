package remote

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/pgm/sply2/core"
	"golang.org/x/net/context"
)

type URLRef struct {
	Owner  *RemoteRefFactoryImp
	Source *core.URLSource
}

func (r *URLRef) GetSize() int64 {
	return r.Source.Size
}

func (r *URLRef) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	req, err := http.NewRequest("GET", r.Source.URL, nil)
	req.Header.Add("If-Match", `"r.ETag"`)
	if offset != 0 || len != r.Source.Size {
		// if r.Source.AcceptsRanges {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+len-1))
		// } else {
		// 	return errors.New("server does not accept ranges")
		// }
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New(fmt.Sprintf("Status code: %d", res.StatusCode))
	}

	n, err := io.Copy(writer, res.Body)
	if err != nil {
		return err
	}

	if n != len {
		return errors.New("Did not copy requested length")
	}

	return nil
}

func (r *URLRef) GetSource() interface{} {
	return r.Source
}

func (r *URLRef) GetChildNodes(ctx context.Context) ([]*core.RemoteFile, error) {
	panic("unimp")
}

func (rrf *RemoteRefFactoryImp) GetHTTPAttr(ctx context.Context, url string) (*core.HTTPAttrs, error) {
	res, err := http.Head(url)
	if err != nil {
		return nil, err
	}
	contentlength := res.ContentLength
	etag := res.Header.Get("ETag")
	// rangeDef := res.Header.Get("Accept-Ranges")
	// acceptRanges := rangeDef == "bytes"

	return &core.HTTPAttrs{ETag: etag, Size: contentlength}, nil
}
