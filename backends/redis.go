package backends

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/utils"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/gomodule/redigo/redis"
)

var RedisDefaultProtocol = `tcp`
var RedisDefaultAddress = `localhost:6379`
var redisDefaultKeyPrefix = `pivot.`
var redisDefaultPingTimeout = 5 * time.Second
var redisDefaultCommandTimeout = 20 * time.Second

type RedisBackend struct {
	Backend
	cs                    dal.ConnectionString
	pool                  *redis.Pool
	registeredCollections sync.Map
	indexer               Indexer
	keyPrefix             string
	timeout               time.Duration
	cmdTimeout            time.Duration
}

func NewRedisBackend(connection dal.ConnectionString) Backend {
	return &RedisBackend{
		cs:        connection,
		keyPrefix: redisDefaultKeyPrefix,
		timeout:   redisDefaultPingTimeout,
	}
}

func (self *RedisBackend) String() string {
	return `redis`
}

func (self *RedisBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *RedisBackend) Ping(timeout time.Duration) error {
	errchan := make(chan error)

	go func() {
		if _, err := self.run(`PING`); err == nil {
			errchan <- nil
		} else {
			errchan <- fmt.Errorf("Backend unavailable: %v", err)
		}
	}()

	select {
	case err := <-errchan:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("Backend unavailable: timed out after waiting %v", timeout)
	}
}

func (self *RedisBackend) RegisterCollection(collection *dal.Collection) {
	log.Debugf("[%v] register collection %q", self, collection.Name)
	self.registeredCollections.Store(collection.Name, collection)
}

func (self *RedisBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *RedisBackend) Initialize() error {
	if self.cs.HasOpt(`prefix`) {
		self.keyPrefix = self.cs.OptString(`prefix`, ``)
	}

	self.timeout = self.cs.OptDuration(`timeout`, redisDefaultPingTimeout)
	self.cmdTimeout = self.cs.OptDuration(`callTimeout`, redisDefaultCommandTimeout)

	if err := self.connect(); err != nil {
		return err
	}

	if self.cs.OptBool(`autoregister`, DefaultAutoregister) {
		if err := self.refreshCollections(); err != nil {
			return err
		}
	}

	if self.indexer == nil {
		self.indexer = self
	}

	if self.indexer != nil {
		if err := self.indexer.IndexInitialize(self); err != nil {
			return err
		}
	}

	return self.Ping(self.timeout)
}

func (self *RedisBackend) Insert(name string, recordset *dal.RecordSet) error {
	return self.upsert(true, name, recordset)
}

func (self *RedisBackend) Exists(name string, id interface{}) bool {
	if collection, err := self.GetCollection(name); err == nil {
		if ids := sliceutil.Sliceify(id); len(ids) == collection.KeyCount() {
			if i, err := redis.Int(self.run(`EXISTS`, self.key(collection.Name, ids...))); err == nil && i == 1 {
				return true
			}
		}
	}

	return false
}

func (self *RedisBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		// expand id, make sure it matches the number of parts we need for this collection
		if ids := sliceutil.Sliceify(id); len(ids) == collection.KeyCount() {
			if dbfields, err := redis.Strings(self.run(`HGETALL`, self.key(collection.Name, ids))); err == nil {
				record := dal.NewRecord(ids[0])

				if len(ids) == 2 {
					if key, ok := collection.GetFirstNonIdentityKeyField(); ok {
						if value, err := key.Format(ids[1], dal.RetrieveOperation); err == nil {
							record.Set(key.Name, value)
						} else {
							return nil, err
						}
					}
				}

				for _, pair := range sliceutil.Chunks(dbfields, 2) {
					if len(pair) == 2 {
						fieldName := fmt.Sprintf("%v", pair[0])
						fieldValue := fmt.Sprintf("%v", pair[1])

						if len(fields) == 0 || sliceutil.ContainsString(fields, fieldName) {
							if value, err := self.decode(collection, fieldName, []byte(fieldValue)); err == nil {
								record.Set(fieldName, value)
							}
						}
					}
				}

				if collection.IdentityFieldType != dal.StringType {
					record.ID = stringutil.Autotype(record.ID)
				}

				// do this AFTER populating the record's fields from the database
				if err := record.Populate(record, collection); err != nil {
					return nil, err
				}

				return record, nil
			} else {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("%v: expected %d key values, got %d", self, collection.KeyCount(), len(ids))
		}
	} else {
		return nil, err
	}
}

func (self *RedisBackend) Update(name string, recordset *dal.RecordSet, target ...string) error {
	return self.upsert(false, name, recordset)
}

func (self *RedisBackend) Delete(name string, ids ...interface{}) error {
	if collection, err := self.GetCollection(name); err == nil {
		var merr error

		keyLen := collection.KeyCount()

		for _, id := range ids {
			if ids := sliceutil.Sliceify(id); keyLen == 0 || len(ids) == keyLen {
				if _, err := self.run(`DEL`, self.key(collection.Name, ids...)); err != nil {
					merr = utils.AppendError(merr, err)
				}
			} else {
				return fmt.Errorf("%v: expected %d key values, got %d", self, keyLen, len(ids))
			}
		}

		return merr
	} else {
		return err
	}
}

func (self *RedisBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	return self.indexer
}

func (self *RedisBackend) WithAggregator(collection *dal.Collection) Aggregator {
	return nil
}

func (self *RedisBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.registeredCollections), nil
}

func (self *RedisBackend) CreateCollection(definition *dal.Collection) error {
	querylog.Debugf("[%v] Create collection %v", self, definition.Name)

	// write the schema definition to the schema key
	if data, err := json.Marshal(definition); err == nil {
		schemaKey := self.key(definition.Name, `__schema__`)

		if out, err := redis.String(self.run(
			`SET`,
			schemaKey,
			data,
			`NX`,
		)); err != nil {
			return err
		} else if out != `OK` {
			return fmt.Errorf("Collection %q already exists", definition.Name)
		}

		self.RegisterCollection(definition)
		return nil
	} else {
		return err
	}
}

func (self *RedisBackend) DeleteCollection(name string) error {
	if collection, err := self.GetCollection(name); err == nil {
		if keys, err := redis.Strings(self.run(`KEYS`, self.key(collection.Name, `*`))); err == nil {
			var merr error

			for _, key := range keys {
				if _, err := self.run(`DEL`, key); err != nil {
					merr = utils.AppendError(merr, err)
				}
			}

			if _, err := self.run(`DEL`, self.key(collection.Name, `__schema__`)); err != nil {
				merr = utils.AppendError(merr, err)
			}

			return merr
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *RedisBackend) GetCollection(name string) (*dal.Collection, error) {
	if collectionI, ok := self.registeredCollections.Load(name); ok && collectionI != nil {
		if i, err := redis.Int(self.run(`EXISTS`, self.key(name, `__schema__`))); err == nil && i == 1 {
			return collectionI.(*dal.Collection), nil
		}
	}

	return nil, dal.CollectionNotFound
}

func (self *RedisBackend) Flush() error {
	return nil
}

func (self *RedisBackend) key(name string, id ...interface{}) string {
	k := self.keyPrefix

	if dataset := self.cs.Dataset(); dataset != `` {
		k += dataset + `.`
	}

	k += name

	for _, str := range sliceutil.Stringify(id) {
		k += `:` + str
	}

	return k
}

func (self *RedisBackend) upsert(create bool, collectionName string, recordset *dal.RecordSet) error {
	if collection, err := self.GetCollection(collectionName); err == nil {
		var merr error
		var ttlSeconds int

		keyLen := collection.KeyCount()

		for _, record := range recordset.Records {
			if r, err := collection.MakeRecord(record); err == nil {
				record = r
			} else {
				return err
			}

			// don't even attempt to write already-expired records
			if collection.IsExpired(record) {
				return nil
			} else {
				ttlSeconds = int(collection.TTL(record).Round(time.Second).Seconds())
			}

			if idLen := len(sliceutil.Sliceify(record.ID)); keyLen > 0 && idLen != keyLen {
				return fmt.Errorf("%v: expected %d key values, got %d", self, keyLen, idLen)
			}

			var key string = self.key(collection.Name, sliceutil.Sliceify(record.ID)...)
			var args []interface{}

			for key, value := range record.Fields {
				if encoded, err := self.encode(collection, key, value); err == nil {
					args = append(args, key)
					args = append(args, encoded)
				}
			}

			if len(args) > 0 {
				if create && self.Exists(collectionName, record.ID) {
					return fmt.Errorf("Record %q already exists", record.ID)
				}

				args = append([]interface{}{key}, args...)

				if out, err := redis.String(self.run(`HMSET`, args...)); err == nil && out == `OK` {
					if ttlSeconds > 0 {
						if _, err := self.run(`EXPIRE`, key, ttlSeconds); err != nil {
							merr = utils.AppendError(merr, err)
						}
					}
				} else if err == nil {
					merr = utils.AppendError(merr, fmt.Errorf("%v: persist failed: %v", self, out))
				} else {
					merr = utils.AppendError(merr, err)
				}
			}
		}

		if search := self.WithSearch(collection); search != nil {
			if err := search.Index(collection, recordset); err != nil {
				merr = utils.AppendError(merr, err)
			}
		}

		return merr
	} else {
		return err
	}
}

func (self *RedisBackend) encode(collection *dal.Collection, key string, value interface{}) ([]byte, error) {
	if _, ok := collection.GetField(key); ok {
		if data, err := json.Marshal(value); err == nil {
			return data, nil
		} else {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("No such field %q", key)
	}
}

func (self *RedisBackend) decode(collection *dal.Collection, key string, value []byte) (interface{}, error) {
	if field, ok := collection.GetField(key); ok {
		switch field.Type {
		case dal.ObjectType:
			var out map[string]interface{}

			if err := json.Unmarshal(value, &out); err == nil {
				return out, nil
			} else {
				return nil, err
			}
		default:
			var out interface{}

			if err := json.Unmarshal(value, &out); err == nil {
				return field.ConvertValue(out)
			} else {
				return nil, err
			}
		}
	} else {
		return nil, fmt.Errorf("No such field %q", key)
	}
}

func (self *RedisBackend) refreshCollections() error {
	if schemata, err := redis.Strings(self.run(`KEYS`, self.key(`*`, `__schema__`))); err == nil {
		var merr error

		for _, key := range schemata {
			merr = utils.AppendError(merr, self.refreshCollection(key))
		}

		return merr
	} else {
		return err
	}
}

func (self *RedisBackend) refreshCollection(schemaKey string) error {
	if collectionName, _ := redisSplitKey(schemaKey); collectionName != `` {
		if _, err := self.GetCollection(collectionName); err == dal.CollectionNotFound {
			if schemadef, err := redis.Bytes(self.run(`GET`, schemaKey)); err == nil {
				var collection dal.Collection

				if err := json.Unmarshal(schemadef, &collection); err == nil {
					if collection.Name != `` {
						self.RegisterCollection(&collection)
						return nil
					} else {
						return fmt.Errorf("Invalid collection schema")
					}
				} else {
					return err
				}
			} else {
				return err
			}
		} else {
			return nil
		}
	} else {
		return fmt.Errorf("invalid key")
	}
}

func redisSplitKey(key string) (string, []string) {
	if parts := strings.Split(key, `.`); len(parts) > 0 {
		final := strings.Split(parts[len(parts)-1], `:`)
		return final[0], final[1:]
	} else {
		return ``, nil
	}
}

// wraps the process of borrowing a connection from the pool and running a command
func (self *RedisBackend) run(cmd string, args ...interface{}) (interface{}, error) {
	if conn := self.pool.Get(); conn != nil {
		defer conn.Close()

		debug := strings.Join(sliceutil.Stringify(args), ` `)

		querylog.Debugf("[%v] %v %v", self, cmd, debug)
		return redis.DoWithTimeout(conn, self.cmdTimeout, cmd, args...)
	} else {
		return nil, fmt.Errorf("Failed to borrow Redis connection")
	}
}

func (self *RedisBackend) connect() error {
	self.pool = &redis.Pool{
		MaxIdle:   3,
		MaxActive: 1024,
		Dial: func() (redis.Conn, error) {
			options := []redis.DialOption{
				redis.DialKeepAlive(self.timeout),
				redis.DialConnectTimeout(self.timeout),
			}

			if _, p, ok := self.cs.Credentials(); ok {
				options = append(options, redis.DialPassword(p))
			}

			return redis.Dial(
				self.cs.Protocol(RedisDefaultProtocol),
				self.cs.Host(RedisDefaultAddress),
				options...,
			)
		},
	}

	return nil
}
