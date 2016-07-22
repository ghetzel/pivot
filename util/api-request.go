package util

import (
	"fmt"
	"github.com/dghubble/sling"
	"io"
	"net/http"
)

type MultiClientRequest struct {
	BodyType RequestBodyType
	request  *sling.Sling
}

func NewClientRequest(method string, path string, payload interface{}, payloadType RequestBodyType) (*MultiClientRequest, error) {
	mcRequest := &MultiClientRequest{
		BodyType: payloadType,
	}

	request := sling.New()

	switch method {
	case `GET`:
		request = request.Get(path)
	case `POST`:
		request = request.Post(path)
	case `PUT`:
		request = request.Put(path)
	case `DELETE`:
		request = request.Delete(path)
	case `HEAD`:
		request = request.Head(path)
	case `PATCH`:
		request = request.Patch(path)
	default:
		return nil, fmt.Errorf("Unsupported HTTP method %q", method)
	}

	if payload != nil {
		switch mcRequest.BodyType {
		case BodyJson:
			request = request.BodyJSON(payload)
		case BodyForm:
			request = request.BodyForm(payload)
		case BodyRaw:
			switch payload.(type) {
			case io.Reader:
				reader := payload.(io.Reader)
				request = request.Body(reader)
			default:
				return nil, fmt.Errorf("Must pass an io.Reader for raw request body")
			}
		}
	}

	mcRequest.request = request

	return mcRequest, nil
}

func (self *MultiClientRequest) Perform(success interface{}, failure interface{}) (*http.Response, error) {
	if self.request == nil {
		return nil, fmt.Errorf("Cannot operated on unconfigured request")
	}

	return self.request.Receive(success, failure)
}

func (self *MultiClientRequest) PerformJSON(output interface{}) error {
	if self.request == nil {
		return fmt.Errorf("Cannot operated on unconfigured request")
	}

	_, err := self.Perform(output, output)
	return err
}
