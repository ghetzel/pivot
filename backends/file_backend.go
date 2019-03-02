package backends

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ghetzel/go-stockutil/fileutil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/typeutil"
	utilutil "github.com/ghetzel/go-stockutil/utils"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type FileBackend struct {
	Backend
	Indexer
	conn        dal.ConnectionString
	recordsets  sync.Map
	collections sync.Map
}

func NewFileBackend(connection dal.ConnectionString) Backend {
	return &FileBackend{
		conn: connection,
	}
}

func (self *FileBackend) filename() string {
	return filepath.Join(self.conn.Host(), self.conn.Dataset())
}

func (self *FileBackend) normalize(filename string) string {
	collection := strings.TrimSpace(filename)
	collection = strings.TrimSuffix(filepath.Base(collection), filepath.Ext(collection))

	return collection
}

func (self *FileBackend) normcol(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	name = strings.TrimFunc(name, func(r rune) bool {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		} else {
			return true
		}
	})

	return name
}

func (self *FileBackend) rs(collection string) *dal.RecordSet {
	if v, ok := self.recordsets.Load(collection); ok {
		if rs, ok := v.(*dal.RecordSet); ok {
			return rs
		}
	}

	return nil
}

func (self *FileBackend) autoregisterCollections() error {
	path := self.filename()

	if fileutil.IsNonemptyFile(path) {
		return self.updateCollectionFromFile(path)
	} else if fileutil.DirExists(path) {
		return fmt.Errorf("Not implemented")
	} else {
		return fmt.Errorf("Must specify a filename or directory")
	}
}

func (self *FileBackend) updateCollectionFromFile(filename string) error {
	switch protocol := self.conn.Protocol(); protocol {
	case `csv`, `tsv`:
		if file, err := os.Open(filename); err == nil {
			defer file.Close()

			csvr := csv.NewReader(file)
			csvr.Comment = '#'

			switch protocol {
			case `csv`:
				csvr.Comma = ','
			case `tsv`:
				csvr.Comma = '\t'
			}

			if rc, err := csvr.ReadAll(); err == nil {
				detectedTypes := make([]utilutil.ConvertType, 0)

				for r, row := range rc {
					if r == 0 {
						continue
					}

					for c, col := range row {
						col = strings.TrimSpace(col)
						dtype := utilutil.DetectConvertType(col)

						if c < len(detectedTypes) {
							// bump the detected type up to less-specific types until we eventually
							// settle on something or everything is just a string
							if dtype.IsSupersetOf(detectedTypes[c]) {
								detectedTypes[c] = dtype
							}
						} else {
							detectedTypes = append(detectedTypes, dtype)
						}
					}
				}

				for _, row := range rc {
					if len(row) > 0 {
						collection := dal.NewCollection(self.normalize(filename))
						collection.IdentityField = ``
						idCol := -1

						for c, col := range row {
							if self.normcol(col) == `id` {
								collection.IdentityField = col
								idCol = c
								break
							}
						}

						if collection.IdentityField == `` {
							collection.IdentityField = strings.TrimSpace(row[0])
							idCol = 0
						}

						for c, col := range row {
							if c == idCol {
								continue
							} else {
								collection.AddFields(dal.Field{
									Name: strings.TrimSpace(col),
									Type: dal.Type(detectedTypes[c].String()),
								})
							}
						}

						self.collections.Store(collection.Name, collection)
					}
				}

				return nil
			} else {
				return fmt.Errorf("read error: %v", err)
			}
		} else {
			return err
		}

	default:
		return fmt.Errorf("Unsupported file layout %q", protocol)
	}
}

func (self *FileBackend) refresh(filename string) error {
	recordset := dal.NewRecordSet()

	if collection, err := self.GetCollection(filename); err == nil {
		switch protocol := self.conn.Protocol(); protocol {
		case `csv`, `tsv`:
			if file, err := os.Open(filename); err == nil {
				defer file.Close()

				csvr := csv.NewReader(file)
				csvr.Comment = '#'

				switch protocol {
				case `csv`:
					csvr.Comma = ','
				case `tsv`:
					csvr.Comma = '\t'
				}

				if rc, err := csvr.ReadAll(); err == nil {
					nameToIndex := make(map[string]int)
					indexToName := make(map[int]string)
					idColName := collection.GetIdentityFieldName()

				NextRow:
					for r, row := range rc {
						record := dal.NewRecord(nil)

					NextColumn:
						for c, col := range row {
							col = strings.TrimSpace(col)

							if col == `` {
								continue NextColumn
							}

							if self.conn.Opt(`header`).Bool() && r == 0 {
								nameToIndex[col] = c
								indexToName[c] = col
							} else if idCol, ok := nameToIndex[idColName]; ok && c == idCol {
								if v, err := collection.ValueForField(idColName, col, dal.RetrieveOperation); err == nil && v != `` {
									record.ID = v
								} else if v == `` {
									log.Warningf("%T: failed to parse identity at R%dC%d: empty ID", self, r, c)
									continue NextRow
								} else {
									log.Warningf("%T: failed to parse identity at R%dC%d: %v", self, r, c, err)
									continue NextRow
								}
							} else if fieldName, ok := indexToName[c]; ok && fieldName != `` {
								if v, err := collection.ValueForField(fieldName, col, dal.RetrieveOperation); err == nil {
									record.Set(fieldName, v)
								} else {
									log.Warningf("%T: failed to parse column R%dC%d: %v", self, r, c, err)
									continue NextRow
								}
							}
						}

						if record.ID != nil {
							recordset.Push(record)
						}
					}

					self.recordsets.Store(self.normalize(collection.Name), recordset)
					return nil
				} else {
					return fmt.Errorf("read error: %v", err)
				}
			} else {
				return err
			}
		default:
			return fmt.Errorf("Unsupported file layout %q", protocol)
		}
	} else {
		return err
	}
}

func (self *FileBackend) Initialize() error {
	if scheme, protocol := self.conn.Scheme(); protocol == `` {
		switch filepath.Ext(self.filename()) {
		case `.csv`:
			protocol = `csv`
		case `.tsv`:
			protocol = `tsv`
		case `.xlsx`:
			protocol = `xlsx`
		default:
			return fmt.Errorf("Could not determine protocol for file %q", self.filename())
		}

		if cs, err := dal.MakeConnectionString(
			scheme+`+`+protocol,
			self.conn.Host(),
			self.conn.Dataset(),
			self.conn.Options,
		); err == nil {
			self.conn = cs
		} else {
			return fmt.Errorf("protocol: %v", err)
		}
	}

	if err := self.autoregisterCollections(); err != nil {
		return err
	}

	return self.refresh(self.filename())
}

func (self *FileBackend) SetIndexer(dal.ConnectionString) error {
	return nil
}

func (self *FileBackend) RegisterCollection(collection *dal.Collection) {
	self.collections.Store(collection.Name, collection)
}

func (self *FileBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

func (self *FileBackend) Exists(collection string, id interface{}) bool {
	if rs := self.rs(collection); rs != nil {
		_, ok := rs.GetRecordByID(id)
		return ok
	}

	return false
}

func (self *FileBackend) Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error) {
	if rs := self.rs(collection); rs != nil {
		if record, ok := rs.GetRecordByID(id); ok {
			return record.OnlyFields(fields), nil
		} else {
			return nil, fmt.Errorf("Record %v does not exist", id)
		}
	} else {
		return nil, dal.CollectionNotFound
	}
}

func (self *FileBackend) Insert(collection string, records *dal.RecordSet) error {
	return fmt.Errorf("File backends are read-only")
}

func (self *FileBackend) Update(collection string, records *dal.RecordSet, target ...string) error {
	return fmt.Errorf("File backends are read-only")
}

func (self *FileBackend) Delete(collection string, ids ...interface{}) error {
	return fmt.Errorf("File backends are read-only")
}

func (self *FileBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("File backends are read-only")
}

func (self *FileBackend) DeleteCollection(collection string) error {
	return fmt.Errorf("File backends are read-only")
}

func (self *FileBackend) ListCollections() ([]string, error) {
	var names []string

	self.collections.Range(func(key interface{}, value interface{}) bool {
		names = append(names, typeutil.String(key))
		return true
	})

	return names, nil
}

func (self *FileBackend) GetCollection(collection string) (*dal.Collection, error) {
	if c, ok := self.collections.Load(collection); ok {
		if collection, ok := c.(*dal.Collection); ok {
			return collection, nil
		}
	}

	return nil, dal.CollectionNotFound
}

func (self *FileBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	return nil
}

func (self *FileBackend) WithAggregator(collection *dal.Collection) Aggregator {
	return nil
}

func (self *FileBackend) Flush() error {
	return nil
}

func (self *FileBackend) Ping(_ time.Duration) error {
	if fn := self.filename(); fileutil.IsNonemptyFile(fn) {
		return nil
	} else {
		return fmt.Errorf("no such file: %s", fn)
	}
}

func (self *FileBackend) String() string {
	return `file`
}

func (self *FileBackend) Supports(features ...BackendFeature) bool {
	for _, feat := range features {
		switch feat {
		default:
			return false
		}
	}

	return true
}
