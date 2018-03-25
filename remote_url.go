package sply2

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/pgm/sply2/core"
	"golang.org/x/net/context"
)

type RemoteURL struct {
	URL           string
	ETag          string
	Length        int64
	AcceptsRanges bool
}

func (r *RemoteURL) GetSize() int64 {
	return r.Length
}

func (r *RemoteURL) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	req, err := http.NewRequest("GET", r.URL, nil)
	req.Header.Add("If-Match", `"r.ETag"`)
	if offset != 0 || len != r.Length {
		if r.AcceptsRanges {
			req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+len-1))
		} else {
			return errors.New("server does not accept ranges")
		}
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
		return errors.New("Did not copy full requested length")
	}

	return nil
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
