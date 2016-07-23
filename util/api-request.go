package util

import (
	"fmt"
	"github.com/dghubble/sling"
	"io"
	"net/http"
	"strings"
)

type MultiClientRequest struct {
	BaseUrl     string
	BodyType    RequestBodyType
	Method      string
	Path        string
	RequestBody interface{}
}

func NewClientRequest(method string, path string, payload interface{}, payloadType RequestBodyType) (*MultiClientRequest, error) {
	mcRequest := &MultiClientRequest{
		BodyType:    payloadType,
		Method:      method,
		Path:        path,
		RequestBody: payload,
	}

	return mcRequest, nil
}

func (self *MultiClientRequest) SetBaseUrl(base string) {
	self.BaseUrl = strings.TrimSuffix(base, `/`) + `/`
}

func (self *MultiClientRequest) Perform(success interface{}, failure interface{}) (*http.Response, error) {
	request := sling.New()

	request.Base(self.BaseUrl)

	switch self.Method {
	case `GET`:
		request.Get(self.Path)
	case `POST`:
		request.Post(self.Path)
	case `PUT`:
		request.Put(self.Path)
	case `DELETE`:
		request.Delete(self.Path)
	case `HEAD`:
		request.Head(self.Path)
	case `PATCH`:
		request.Patch(self.Path)
	default:
		return nil, fmt.Errorf("Unsupported HTTP method %q", self.Method)
	}

	if self.RequestBody != nil {
		switch self.BodyType {
		case BodyJson:
			request.BodyJSON(self.RequestBody)
		case BodyForm:
			request.BodyForm(self.RequestBody)
		case BodyRaw:
			switch self.RequestBody.(type) {
			case io.Reader:
				reader := self.RequestBody.(io.Reader)
				request.Body(reader)
			default:
				return nil, fmt.Errorf("Must pass an io.Reader for raw request body")
			}
		}
	}

	return request.Receive(success, failure)
}
