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

var fileBackendTypeAutodetectRows = 50
var fileBackendAutoindexField = `_index`

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
	filename := filepath.Join(self.conn.Host(), self.conn.Dataset())
	filename = fileutil.MustExpandUser(filename)

	return filename
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
	protocol := self.conn.Protocol()
	log.Debugf("%T: autoregister %s (protocol: %s)", self, filename, protocol)

	switch protocol {
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
						} else if dtype.String() == `` {
							detectedTypes = append(detectedTypes, utilutil.String)
						} else {
							detectedTypes = append(detectedTypes, dtype)
						}
					}

					if r > fileBackendTypeAutodetectRows {
						break
					}
				}

				collection := dal.NewCollection(self.normalize(filename))
				collection.SourceURI = filename

				for _, row := range rc {
					if len(row) > 0 {
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
							collection.IdentityField = fileBackendAutoindexField
						}

						for c, col := range row {
							if c == idCol {
								collection.IdentityFieldType = dal.Type(detectedTypes[c].String())
								collection.IdentityFieldIndex = c
								continue
							} else {
								collection.AddFields(dal.Field{
									Name:  strings.TrimSpace(col),
									Type:  dal.Type(detectedTypes[c].String()),
									Index: c,
								})
							}
						}
					}

					break
				}

				self.collections.Store(collection.Name, collection)

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
					idColName := collection.GetIdentityFieldName()
					hasExplicitIdColumn := (idColName != fileBackendAutoindexField)

				NextRow:
					for r, row := range rc {
						record := dal.NewRecord(r)

					NextColumn:
						for c, col := range row {
							col = strings.TrimSpace(col)

							if col == `` {
								continue NextColumn
							}

							if r == 0 {
								continue NextRow
							} else if hasExplicitIdColumn && c == collection.IdentityFieldIndex {
								if v, err := collection.ValueForField(idColName, col, dal.RetrieveOperation); err == nil && v != `` {
									record.ID = v
								} else if v == `` {
									log.Warningf("%T: failed to parse identity at R%dC%d: empty ID", self, r, c)
									continue NextRow
								} else {
									log.Warningf("%T: failed to parse identity at R%dC%d: %v", self, r, c, err)
									continue NextRow
								}
							} else if field, ok := collection.GetFieldByIndex(c); ok {
								if v, err := field.ConvertValue(col); err == nil {
									record.SetNested(field.Name, v)
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
		return fmt.Errorf("autoregister failed: %v", err)
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

func (self *FileBackend) Exists(name string, id interface{}) bool {
	if collection, err := self.GetCollection(name); err == nil {
		self.refresh(collection.SourceURI)

		if rs := self.rs(collection.Name); rs != nil {
			_, ok := rs.GetRecordByID(id)
			return ok
		}
	}

	return false
}

func (self *FileBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if err := self.refresh(collection.SourceURI); err != nil {
			return nil, err
		}

		if rs := self.rs(collection.Name); rs != nil {
			if record, ok := rs.GetRecordByID(id); ok {
				return record.OnlyFields(fields), nil
			} else {
				return nil, fmt.Errorf("Record %v does not exist", id)
			}
		}
	}

	return nil, dal.CollectionNotFound
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
	if c, ok := self.collections.Load(self.normalize(collection)); ok {
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
