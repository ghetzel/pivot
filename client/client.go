package client

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/util"
)

const DefaultPivotUrl = `http://localhost:29029`

type Status = util.Status

type QueryOptions struct {
	Limit       int      `json:"limit"`
	Offset      int      `json:"offset"`
	Sort        []string `json:"sort,omitempty"`
	Fields      []string `json:"fields,omitempty"`
	Conjunction string   `json:"conjunction,omitempty"`
}

type Pivot struct {
	*httputil.Client
}

func New(url string) (*Pivot, error) {
	if url == `` {
		url = DefaultPivotUrl
	}

	if client, err := httputil.NewClient(url); err == nil {
		return &Pivot{
			Client: client,
		}, nil
	} else {
		return nil, err
	}
}

func (self *Pivot) Status() (*Status, error) {
	if response, err := self.Get(`/api/status`, nil, nil); err == nil {
		status := Status{}

		if err := self.Decode(response.Body, &status); err == nil {
			return &status, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) Collections() ([]string, error) {
	if response, err := self.Get(`/api/schema`, nil, nil); err == nil {
		var names []string

		if err := self.Decode(response.Body, &names); err == nil {
			return names, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) CreateCollection(def *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *Pivot) DeleteCollection(name string) error {
	_, err := self.Delete(fmt.Sprintf("/api/schema/%s", name), nil, nil)
	return err
}

func (self *Pivot) Collection(name string) (*dal.Collection, error) {
	if response, err := self.Get(fmt.Sprintf("/api/schema/%s", name), nil, nil); err == nil {
		var collection dal.Collection

		if err := self.Decode(response.Body, &collection); err == nil {
			return &collection, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) Query(collection string, query interface{}, options *QueryOptions) (*dal.RecordSet, error) {
	var response *http.Response
	var err error

	opts := make(map[string]interface{})

	if options != nil {
		opts[`limit`] = options.Limit
		opts[`offset`] = options.Offset

		if len(options.Sort) > 0 {
			opts[`sort`] = strings.Join(options.Sort, `,`)
		}

		if len(options.Fields) > 0 {
			opts[`fields`] = strings.Join(options.Fields, `,`)
		}
	}

	if typeutil.IsMap(query) {
		response, err = self.Post(fmt.Sprintf("/api/collections/%s/query/", collection), query, opts, nil)
	} else if typeutil.IsArray(query) {
		qS := sliceutil.Stringify(query)

		if len(qS) == 0 {
			qS = []string{`all`}
		}

		response, err = self.Get(fmt.Sprintf("/api/collections/%s/where/%s", collection, strings.Join(qS, `/`)), opts, nil)
	} else {
		q := typeutil.String(query)

		if q == `` {
			q = `all`
		}

		response, err = self.Get(fmt.Sprintf("/api/collections/%s/where/%s", collection, q), opts, nil)
	}

	if err == nil {
		var recordset dal.RecordSet

		if err := self.Decode(response.Body, &recordset); err == nil {
			return &recordset, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) Aggregate(collection string, query interface{}) (*dal.RecordSet, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (self *Pivot) GetRecord(collection string, id interface{}) (*dal.Record, error) {
	if response, err := self.Get(fmt.Sprintf("/api/collections/%s/records/%v", collection, id), nil, nil); err == nil {
		var record dal.Record

		if err := self.Decode(response.Body, &record); err == nil {
			return &record, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) CreateRecord(collection string, records ...*dal.Record) (*dal.RecordSet, error) {
	return self.upsertRecord(true, collection, records...)
}

func (self *Pivot) UpdateRecord(collection string, records ...*dal.Record) (*dal.RecordSet, error) {
	return self.upsertRecord(false, collection, records...)
}

func (self *Pivot) upsertRecord(create bool, collection string, records ...*dal.Record) (*dal.RecordSet, error) {
	var response *http.Response
	var err error

	recordset := dal.NewRecordSet(records...)

	if create {
		response, err = self.Post(fmt.Sprintf("/api/collections/%s", collection), recordset, nil, nil)
	} else {
		response, err = self.Put(fmt.Sprintf("/api/collections/%s", collection), recordset, nil, nil)
	}

	if err == nil {
		if err := self.Decode(response.Body, recordset); err == nil {
			return recordset, nil
		} else {
			return recordset, err
		}
	} else {
		return nil, err
	}
}

func (self *Pivot) DeleteRecords(collection string, ids ...interface{}) error {
	_, err := self.Delete(fmt.Sprintf(
		"/api/collections/%s/records/%s",
		collection,
		strings.Join(sliceutil.Stringify(ids), `/`),
	), nil, nil)
	return err
}
