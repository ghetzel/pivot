package backends

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghodss/yaml"
	"github.com/hashicorp/golang-lru"
)

var WriteLockFormat = `%s.lock`
var FilesystemRecordCacheSize = 1024
var RecordCacheEnabled = false

const DefaultFilesystemRecordSubdirectory = `data`

type SerializationFormat string

const (
	FormatYAML SerializationFormat = `yaml`
	FormatJSON                     = `json`
	FormatCSV                      = `csv`
)

type FilesystemBackend struct {
	Backend
	Indexer
	conn                  dal.ConnectionString
	root                  string
	format                SerializationFormat
	indexer               Indexer
	aggregator            map[string]Aggregator
	options               ConnectOptions
	registeredCollections map[string]*dal.Collection
	recordSubdir          string
	recordCache           *lru.ARCCache
}

func NewFilesystemBackend(connection dal.ConnectionString) Backend {
	return &FilesystemBackend{
		conn:                  connection,
		format:                FormatYAML,
		aggregator:            make(map[string]Aggregator),
		registeredCollections: make(map[string]*dal.Collection),
		recordSubdir:          DefaultFilesystemRecordSubdirectory,
	}
}

func (self *FilesystemBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

func (self *FilesystemBackend) RegisterCollection(collection *dal.Collection) {
	self.registeredCollections[collection.Name] = collection
}

func (self *FilesystemBackend) SetOptions(options ConnectOptions) {
	self.options = options
}

func (self *FilesystemBackend) Initialize() error {
	switch self.conn.Protocol() {
	case `yaml`:
		self.format = FormatYAML
	case `json`:
		self.format = FormatJSON
	case `csv`:
		self.format = FormatCSV
	case ``:
		break
	default:
		return fmt.Errorf("Unknown serialization format %q", self.conn.Protocol())
	}

	self.root = self.conn.Dataset()

	// expand the path
	if strings.HasPrefix(self.root, `/.`) {
		self.root = strings.TrimPrefix(self.root, `/`)
	} else if strings.HasPrefix(self.root, `/~`) {
		self.root = strings.TrimPrefix(self.root, `/`)

		if v, err := pathutil.ExpandUser(self.root); err == nil {
			self.root = v
		} else {
			return err
		}
	}

	// absolutify the path
	if v, err := filepath.Abs(self.root); err == nil {
		self.root = v
	} else {
		return err
	}

	// validate or create the parent directory
	if stat, err := os.Stat(self.root); err == nil {
		if !stat.IsDir() {
			return fmt.Errorf("Root path %q exists, but is not a directory", self.root)
		}
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(self.root, 0700); err != nil {
			return err
		}
	} else {
		return err
	}

	var primaryIndexer Indexer

	// setup indexer
	if indexConnString := self.options.Indexer; indexConnString != `` {
		if ics, err := dal.ParseConnectionString(indexConnString); err == nil {
			if indexer, err := MakeIndexer(ics); err == nil {
				if err := indexer.IndexInitialize(self); err == nil {
					primaryIndexer = indexer
				}else{
					return err
				}
			}else{
				return err
			}
		}else{
			return err
		}
	} else {
		primaryIndexer = self
	}

	// setup additional indexers
	if len(self.options.AdditionalIndexers) > 0 {
		multi := NewMultiIndex()
		multi.AddIndexer(primaryIndexer)

		for _, addlIndexerConnString := range self.options.AdditionalIndexers {
			if err := multi.AddIndexerByConnectionString(addlIndexerConnString); err != nil {
				return err
			}
		}

		if err := multi.IndexInitialize(self); err == nil {
			self.indexer = multi
		} else {
			return err
		}
	} else {
		self.indexer = primaryIndexer
	}

	if arc, err := lru.NewARC(FilesystemRecordCacheSize); err == nil {
		self.recordCache = arc
	} else {
		return err
	}

	return nil
}

func (self *FilesystemBackend) Insert(collectionName string, recordset *dal.RecordSet) error {
	for _, record := range recordset.Records {
		if self.Exists(collectionName, record.ID) {
			return fmt.Errorf("Record %q already exists", record.ID)
		}
	}

	return self.Update(collectionName, recordset)
}

func (self *FilesystemBackend) Exists(name string, id interface{}) bool {
	if collection, ok := self.registeredCollections[name]; ok {
		if dataRoot, err := self.getDataRoot(collection.Name, true); err == nil {
			if filename := self.makeFilename(collection, fmt.Sprintf("%v", id), true); filename != `` {
				if stat, err := os.Stat(filepath.Join(dataRoot, filename)); err == nil {
					if stat.Size() > 0 {
						return true
					}
				}
			}
		}
	}

	return false
}

func (self *FilesystemBackend) Retrieve(collectionName string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, ok := self.registeredCollections[collectionName]; ok {
		var record dal.Record

		if err := self.readObject(collection, fmt.Sprintf("%v", id), true, &record); err == nil {
			self.prepareIncomingRecord(collection.Name, &record)

			// add/touch item in cache for rapid readback if necessary
			self.recordCache.Add(fmt.Sprintf("%v|%v", collectionName, record.ID), &record)

			return &record, nil
		} else {
			return nil, err
		}
	} else {
		return nil, dal.CollectionNotFound
	}
}

func (self *FilesystemBackend) Update(collectionName string, recordset *dal.RecordSet, target ...string) error {
	if collection, ok := self.registeredCollections[collectionName]; ok {
		for _, record := range recordset.Records {
			if err := self.writeObject(collection, fmt.Sprintf("%v", record.ID), true, record); err != nil {
				return err
			}

			// add/touch item in cache for rapid readback if necessary
			self.recordCache.Add(fmt.Sprintf("%v|%v", collectionName, record.ID), record)
		}

		if search := self.WithSearch(collection.Name); search != nil {
			if err := search.Index(collection.Name, recordset); err != nil {
				return err
			}
		}

		return nil
	} else {
		return dal.CollectionNotFound
	}
}

func (self *FilesystemBackend) Delete(collectionName string, ids ...interface{}) error {
	if collection, ok := self.registeredCollections[collectionName]; ok {
		// remove documents from index
		if search := self.WithSearch(collection.Name); search != nil {
			defer search.IndexRemove(collection.Name, ids)
		}

		if dataRoot, err := self.getDataRoot(collection.Name, true); err == nil {
			for _, id := range ids {
				if filename := self.makeFilename(collection, fmt.Sprintf("%v", id), true); filename != `` {
					os.Remove(filepath.Join(dataRoot, filename))
				}

				// explicitly remove item from cache
				self.recordCache.Remove(fmt.Sprintf("%v|%v", collectionName, id))
			}

			return nil
		} else {
			return err
		}
	} else {
		return dal.CollectionNotFound
	}
}

func (self *FilesystemBackend) WithSearch(collectionName string, filters ...filter.Filter) Indexer {
	return self.indexer
}

func (self *FilesystemBackend) WithAggregator(collectionName string) Aggregator {
	return nil
}

func (self *FilesystemBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(self.registeredCollections), nil
}

func (self *FilesystemBackend) CreateCollection(definition *dal.Collection) error {
	if err := self.writeObject(definition, `schema`, false, definition); err == nil {
		self.RegisterCollection(definition)
		return nil
	} else {
		return err
	}
}

func (self *FilesystemBackend) DeleteCollection(collectionName string) error {
	if _, ok := self.registeredCollections[collectionName]; ok {
		if datadir, err := self.getDataRoot(collectionName, false); err == nil {
			if _, err := os.Stat(datadir); os.IsNotExist(err) {
				return nil
			}

			return os.RemoveAll(datadir)
		} else {
			return err
		}
	} else {
		return dal.CollectionNotFound
	}
}

func (self *FilesystemBackend) GetCollection(name string) (*dal.Collection, error) {
	if collection, ok := self.registeredCollections[name]; ok {
		var v map[string]interface{}

		if err := self.readObject(collection, `schema`, false, v); err == nil {
			return collection, nil
		}
	}

	return nil, dal.CollectionNotFound
}

func (self *FilesystemBackend) getDataRoot(collectionName string, isData bool) (string, error) {
	var dataRoot string

	if isData {
		dataRoot = filepath.Join(self.root, collectionName, self.recordSubdir)
	} else {
		dataRoot = filepath.Join(self.root, collectionName)
	}

	if _, err := os.Stat(dataRoot); os.IsNotExist(err) {
		if err := os.MkdirAll(dataRoot, 0700); err != nil {
			return ``, err
		}
	} else if err != nil {
		return ``, err
	}

	return filepath.Clean(dataRoot), nil
}

func (self *FilesystemBackend) makeFilename(collection *dal.Collection, id string, isData bool) string {
	var filename string

	switch self.format {
	case FormatYAML:
		filename = fmt.Sprintf("%v.yaml", id)

	case FormatJSON:
		filename = fmt.Sprintf("%v.json", id)

	case FormatCSV:
		filename = fmt.Sprintf("%v.csv", id)
	}

	return filename
}

func (self *FilesystemBackend) writeObject(collection *dal.Collection, id string, isData bool, value interface{}) error {
	if dataRoot, err := self.getDataRoot(collection.Name, isData); err == nil {
		id = filepath.Base(filepath.Clean(id))

		if filename := self.makeFilename(collection, id, isData); filename != `` {
			if isData {
				hashdir := filepath.Join(dataRoot, filepath.Dir(filename))

				if _, err := os.Stat(hashdir); os.IsNotExist(err) {
					if err := os.MkdirAll(hashdir, 0700); err != nil {
						return err
					}
				}
			}

			var data []byte

			switch self.format {
			case FormatYAML:
				if d, err := yaml.Marshal(value); err == nil {
					data = d
				} else {
					return err
				}

			case FormatJSON:
				if d, err := json.MarshalIndent(value, ``, `  `); err == nil {
					data = d
				} else {
					return err
				}

			case FormatCSV:
				return fmt.Errorf("Not Implemented")
			}

			lockfilename := filepath.Join(dataRoot, fmt.Sprintf(WriteLockFormat, id))

			if _, err := os.Stat(lockfilename); os.IsNotExist(err) {
				if lockfile, err := os.Create(lockfilename); err == nil {
					defer lockfile.Close()
					defer os.Remove(lockfilename)

					if _, err := lockfile.Write([]byte(fmt.Sprintf("%v", time.Now().UnixNano()))); err == nil {
						if file, err := os.Create(filepath.Join(dataRoot, filename)); err == nil {
							defer file.Close()

							// querylog.Debugf("[%T] Write to %v: %v", self, file.Name(), string(data))

							// write the data
							_, err := file.Write(data)
							os.Remove(lockfilename)

							return err
						} else {
							return err
						}
					} else {
						return err
					}
				} else {
					return err
				}
			} else if os.IsExist(err) {
				return fmt.Errorf("Record %v/%v is already locked", collection.Name, id)
			} else {
				return err
			}
		} else {
			return fmt.Errorf("Invalid filename")
		}
	} else {
		return err
	}
}

func (self *FilesystemBackend) listObjectIdsInCollection(collection *dal.Collection) ([]string, error) {
	ids := make([]string, 0)

	if dataRoot, err := self.getDataRoot(collection.Name, true); err == nil {
		if entries, err := ioutil.ReadDir(dataRoot); err == nil {
			for _, entry := range entries {
				basename := filepath.Base(entry.Name())
				baseNoExt := strings.TrimSuffix(basename, filepath.Ext(entry.Name()))

				if filename := self.makeFilename(collection, baseNoExt, true); filename == basename {
					ids = append(ids, baseNoExt)
				}
			}
		} else {
			return ids, err
		}
	} else {
		return ids, err
	}

	return ids, nil
}

func (self *FilesystemBackend) readObject(collection *dal.Collection, id string, isData bool, into interface{}) error {
	if RecordCacheEnabled && isData && into != nil {
		if record, ok := into.(*dal.Record); ok {
			if cacheRecordI, ok := self.recordCache.Get(fmt.Sprintf("%v|%v", collection.Name, id)); ok {
				if cacheRecord, ok := cacheRecordI.(*dal.Record); ok && cacheRecord != nil {
					record.Copy(cacheRecord)
					querylog.Debugf("[%T] Record %v/%v read from cache", self, collection.Name, id)
					return nil
				}
			}
		}
	}

	if dataRoot, err := self.getDataRoot(collection.Name, isData); err == nil {
		if filename := self.makeFilename(collection, id, isData); filename != `` {
			if file, err := os.Open(filepath.Join(dataRoot, filename)); err == nil {
				defer file.Close()
				querylog.Debugf("[%T] Record %v/%v read from disk", self, collection.Name, id)

				if data, err := ioutil.ReadAll(file); err == nil {
					switch self.format {
					case FormatYAML:
						if err := yaml.Unmarshal(data, &into); err != nil {
							return err
						}

					case FormatJSON:
						if err := json.Unmarshal(data, &into); err != nil {
							return err
						}

					case FormatCSV:
						return fmt.Errorf("Not Implemented")
					}
				} else {
					return err
				}
			} else if os.IsNotExist(err) {
				// if it doesn't exist, make sure it's not indexed
				if search := self.WithSearch(collection.Name); search != nil {
					defer search.IndexRemove(collection.Name, []interface{}{id})
				}

				return fmt.Errorf("Record %v does not exist", id)
			} else {
				return err
			}
		} else {
			return fmt.Errorf("Invalid filename")
		}
	} else {
		return err
	}

	return nil
}

func (self *FilesystemBackend) prepareIncomingRecord(collectionName string, record *dal.Record) {
	if collection, ok := self.registeredCollections[collectionName]; ok {
		if collection.IdentityFieldType != dal.StringType {
			record.ID = stringutil.Autotype(record.ID)
		}

		for _, field := range collection.Fields {
			value := record.Get(field.Name)

			if !ok || typeutil.IsZero(value) {
				if field.DefaultValue != nil {
					record.Set(field.Name, field.GetDefaultValue())
				} else if field.Required {
					record.Set(field.Name, field.GetTypeInstance())
				}
			}
		}
	}
}