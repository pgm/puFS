package sply2

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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

func (r *RemoteURL) Copy(offset int64, len int64, writer io.Writer) error {
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

func getURLAttr(url string) (string, int64, error) {
	res, err := http.Head(url)
	if err != nil {
		return "", 0, err
	}
	contentlength := res.ContentLength
	etag := res.Header.Get("ETag")
	// rangeDef := res.Header.Get("Accept-Ranges")
	// acceptRanges := rangeDef == "bytes"

	return etag, contentlength, nil
}

// func ensureRemotePopulated(node *Node) error {
// 	if node.Remote != nil {
// 		return nil
// 	}

// 	remote, etag, size, err := NewRemoteURL(node.URL)
// 	if err != nil {
// 		return err
// 	}

// 	node.Remote, node.ETag, node.Size = remote, etag, size

// 	return nil
// }
