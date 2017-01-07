package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"reflect"
	"strings"
	"sync"
)

type sqlTableDetails struct {
	Index        int
	Name         string
	Type         string
	TypeLength   int
	Precision    int
	NativeType   string
	PrimaryKey   bool
	KeyField     bool
	Nullable     bool
	Unique       bool
	DefaultValue string
}

type sqlTableDetailsFunc func(datasetName string, collectionName string) (*dal.Collection, error)

type SqlBackend struct {
	Backend
	Indexer
	conn                           dal.ConnectionString
	db                             *sql.DB
	indexer                        Indexer
	options                        ConnectOptions
	queryGenTypeMapping            generators.SqlTypeMapping
	queryGenPlaceholderArgument    string
	queryGenPlaceholderFormat      string
	queryGenTableFormat            string
	queryGenFieldFormat            string
	queryGenStringNormalizerFormat string
	listAllTablesQuery             string
	createPrimaryKeyIntFormat      string
	createPrimaryKeyStrFormat      string
	showTableDetailQuery           string
	refreshCollectionFunc          sqlTableDetailsFunc
	dropTableQuery                 string
	collectionCache                map[string]*dal.Collection
	collectionCacheLock            sync.RWMutex
}

func NewSqlBackend(connection dal.ConnectionString) *SqlBackend {
	return &SqlBackend{
		conn:                      connection,
		queryGenTypeMapping:       generators.DefaultSqlTypeMapping,
		queryGenPlaceholderFormat: `?`,
		collectionCache:           make(map[string]*dal.Collection),
		dropTableQuery:            `DROP TABLE %s`,
	}
}

func (self *SqlBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

func (self *SqlBackend) SetOptions(options ConnectOptions) {
	self.options = options
}

func (self *SqlBackend) Initialize() error {
	backend := self.conn.Backend()
	internalBackend := backend

	var name string
	var dsn string
	var err error

	// setup driver-specific settings
	switch backend {
	case `sqlite`:
		name, dsn, err = self.initializeSqlite()
	case `mysql`:
		name, dsn, err = self.initializeMysql()
	default:
		return fmt.Errorf("Unsupported backend %q", backend)
	}

	if err != nil {
		return err
	} else if name != `` {
		internalBackend = name
	}

	log.Debugf("SQL: driver=%s, dsn=%s", internalBackend, dsn)

	// setup the database driver for use
	if db, err := sql.Open(internalBackend, dsn); err == nil {
		self.db = db
	} else {
		return err
	}

	// actually verify database connectivity at this time
	if err := self.db.Ping(); err != nil {
		return err
	}

	// refresh schema cache
	if err := self.refreshAllCollections(); err != nil {
		return err
	}

	// setup indexer (if not using ourself as the default)
	if indexConnString := self.options.Indexer; indexConnString != `` {
		if ics, err := dal.ParseConnectionString(indexConnString); err == nil {
			if indexer, err := MakeIndexer(ics); err == nil {
				if err := indexer.IndexInitialize(self); err == nil {
					self.indexer = indexer
					log.Debugf("Search indexing enabled for %T backend at %q", self, self.indexer.IndexConnectionString())
				} else {
					return err
				}
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		self.indexer = self
	}

	return nil
}

func (self *SqlBackend) Insert(name string, recordset *dal.RecordSet) error {
	if collection, err := self.getCollectionFromCache(name); err == nil {

		if tx, err := self.db.Begin(); err == nil {
			switch self.conn.Backend() {
			case `mysql`:
				// disable zero-means-use-autoincrement for inserts in MySQL
				if _, err := tx.Exec(`SET sql_mode='NO_AUTO_VALUE_ON_ZERO'`); err != nil {
					defer tx.Rollback()
					return err
				}
			}

			// for each record being inserted...
			for _, record := range recordset.Records {
				// setup query generator
				queryGen := self.makeQueryGen()
				queryGen.Type = generators.SqlInsertStatement

				// add record data to query input
				for k, v := range record.Fields {
					// convert incoming values to their destination field types
					if cV, err := collection.ConvertValue(k, v); err == nil {
						queryGen.InputData[k] = cV
					} else {
						defer tx.Rollback()
						return err
					}
				}

				// set the primary key
				if record.ID != `` {
					// convert incoming ID to it's destination field type
					if v, err := collection.ConvertValue(collection.IdentityField, record.ID); err == nil {
						queryGen.InputData[collection.IdentityField] = v
					} else {
						defer tx.Rollback()
						return err
					}
				}

				// render the query into the final SQL
				if stmt, err := filter.Render(queryGen, collection.Name, filter.Null); err == nil {
					// execute the SQL
					if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err != nil {
						defer tx.Rollback()
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			}

			// commit transaction
			if err := tx.Commit(); err == nil {
				if search := self.WithSearch(); search != nil {
					if err := search.Index(collection.Name, recordset); err != nil {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) Exists(name string, id interface{}) bool {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if tx, err := self.db.Begin(); err == nil {
			defer tx.Commit()

			if f, err := filter.FromMap(map[string]interface{}{
				collection.IdentityField: id,
			}); err == nil {
				f.Fields = []string{collection.IdentityField}
				queryGen := self.makeQueryGen()

				if err := queryGen.Initialize(collection.Name); err == nil {
					if sqlString, err := filter.Render(queryGen, collection.Name, f); err == nil {
						// perform query
						row := tx.QueryRow(string(sqlString[:]), queryGen.GetValues()...)
						var outId interface{}

						if err := row.Scan(&outId); err == nil {
							return (id == outId)
						}
					}
				}
			}
		}
	}

	return false
}

func (self *SqlBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if f, err := filter.FromMap(map[string]interface{}{
			collection.IdentityField: id,
		}); err == nil {
			f.Fields = fields
			queryGen := self.makeQueryGen()

			if err := queryGen.Initialize(collection.Name); err == nil {
				if sqlString, err := filter.Render(queryGen, collection.Name, f); err == nil {
					// perform query
					if rows, err := self.db.Query(string(sqlString[:]), queryGen.GetValues()...); err == nil {
						defer rows.Close()

						if columns, err := rows.Columns(); err == nil {
							if rows.Next() {
								return self.scanFnValueToRecord(collection, columns, reflect.ValueOf(rows.Scan))
							} else {
								return nil, fmt.Errorf("Record %s does not exist", id)
							}
						} else {
							return nil, err
						}
					} else {
						return nil, err
					}
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *SqlBackend) Update(name string, recordset *dal.RecordSet, target ...string) error {
	var targetFilter filter.Filter

	if len(target) > 0 {
		if f, err := filter.Parse(target[0]); err == nil {
			targetFilter = f
		} else {
			return err
		}
	}

	if collection, err := self.getCollectionFromCache(name); err == nil {
		if tx, err := self.db.Begin(); err == nil {
			// for each record being updated...
			for _, record := range recordset.Records {
				// setup query generator
				queryGen := self.makeQueryGen()
				queryGen.Type = generators.SqlUpdateStatement

				var recordUpdateFilter filter.Filter

				// if this record was specified without a specific ID, attempt to use the broader
				// target filter (if given)
				if record.ID == `` {
					if len(target) > 0 {
						recordUpdateFilter = targetFilter
					} else {
						defer tx.Rollback()
						return fmt.Errorf("Update must target at least one record")
					}
				} else {
					// try to build a filter targeting this specific record
					if f, err := filter.FromMap(map[string]interface{}{
						collection.IdentityField: record.ID,
					}); err == nil {
						recordUpdateFilter = f
					} else {
						defer tx.Rollback()
						return err
					}
				}

				// add all non-ID fields to the record's Fields set
				for k, v := range record.Fields {
					if k != collection.IdentityField {
						queryGen.InputData[k] = v
					}
				}

				// generate SQL
				if stmt, err := filter.Render(queryGen, collection.Name, recordUpdateFilter); err == nil {
					// execute SQL
					if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err != nil {
						defer tx.Rollback()
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			}

			if err := tx.Commit(); err == nil {
				if search := self.WithSearch(); search != nil {
					if err := search.Index(collection.Name, recordset); err != nil {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) Delete(name string, ids ...interface{}) error {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		f := filter.MakeFilter()

		f.AddCriteria(filter.Criterion{
			Field:  collection.IdentityField,
			Values: ids,
		})

		if tx, err := self.db.Begin(); err == nil {
			queryGen := self.makeQueryGen()
			queryGen.Type = generators.SqlDeleteStatement

			// generate SQL
			if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
				// execute SQL
				if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err == nil {
					if err := tx.Commit(); err == nil {
						if search := self.WithSearch(); search != nil {
							// remove documents from index
							return search.IndexRemove(collection.Name, ids)
						} else {
							return nil
						}
					} else {
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			} else {
				defer tx.Rollback()
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) WithSearch() Indexer {
	return self.indexer
}

func (self *SqlBackend) ListCollections() ([]string, error) {
	if err := self.refreshAllCollections(); err == nil {
		return maputil.StringKeys(self.collectionCache), nil
	} else {
		return nil, err
	}
}

func (self *SqlBackend) CreateCollection(definition *dal.Collection) error {
	// -- sqlite3
	// CREATE TABLE foo (
	//     "id"         INTEGER PRIMARY KEY ASC,
	//     "name"       TEXT NOT NULL,
	//     "enabled"    INTEGER(1),
	//     "created_at" TEXT DEFAULT CURRENT_TIMESTAMP
	// );

	// -- MySQL
	// CREATE TABLE foo (
	//     `id`         INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
	//     `name`       TEXT NOT NULL,
	//     `enabled`    TINYINT(1),
	//     `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
	// );

	// -- PostgreSQL
	// CREATE TABLE foo (
	//     "id"         BIGSERIAL PRIMARY KEY,
	//     "name"       TEXT NOT NULL,
	//     "enabled"    BOOLEAN,
	//     "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now_utc()
	// );

	// -- MS SQL Server
	// CREATE TABLE [foo] (
	//     [id]         INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	//     [name]       NVARCHAR(MAX) NOT NULL,
	//     [enabled     BIT,
	//     [created_at] [DATETIME] DEFAULT CURRENT_TIMESTAMP
	// );

	if definition.IdentityField == `` {
		definition.IdentityField = dal.DefaultIdentityField
	}

	gen := self.makeQueryGen()

	query := fmt.Sprintf("CREATE TABLE %s (", gen.ToTableName(definition.Name))

	fields := []string{}
	values := make([]interface{}, 0)

	if definition.IdentityField != `` {
		switch definition.IdentityFieldType {
		case `str`:
			fields = append(fields, fmt.Sprintf(self.createPrimaryKeyStrFormat, gen.ToFieldName(definition.IdentityField)))
		default:
			fields = append(fields, fmt.Sprintf(self.createPrimaryKeyIntFormat, gen.ToFieldName(definition.IdentityField)))
		}
	}

	for _, field := range definition.Fields {
		var def string

		if nativeType, err := gen.ToNativeType(field.Type, field.Length); err == nil {
			def = fmt.Sprintf("%s %s", gen.ToFieldName(field.Name), nativeType)
		} else {
			return err
		}

		if field.Properties != nil {
			if field.Properties.Required {
				def += ` NOT NULL`
			}

			if field.Properties.Unique {
				def += ` UNIQUE`
			}

			if v := field.Properties.DefaultValue; v != nil {
				def += `DEFAULT ?`
				values = append(values, v)
			}
		}

		fields = append(fields, def)
	}

	query += strings.Join(fields, `, `)
	query += `)`

	if tx, err := self.db.Begin(); err == nil {
		if _, err := tx.Exec(query, values...); err == nil {
			defer self.refreshAllCollections()
			return tx.Commit()
		} else {
			defer tx.Rollback()
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) DeleteCollection(collection string) error {
	gen := self.makeQueryGen()

	if tx, err := self.db.Begin(); err == nil {
		query := fmt.Sprintf(self.dropTableQuery, gen.ToTableName(collection))

		if _, err := tx.Exec(query); err == nil {
			return tx.Commit()
		} else {
			defer tx.Rollback()
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) GetCollection(name string) (*dal.Collection, error) {
	if err := self.refreshCollection(name); err == nil {
		if collection, err := self.getCollectionFromCache(name); err == nil {
			return collection, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *SqlBackend) makeQueryGen() *generators.Sql {
	queryGen := generators.NewSqlGenerator()
	queryGen.TypeMapping = self.queryGenTypeMapping

	if v := self.queryGenPlaceholderFormat; v != `` {
		queryGen.PlaceholderFormat = v
	}

	if v := self.queryGenPlaceholderArgument; v != `` {
		queryGen.PlaceholderArgument = v
	}

	if v := self.queryGenTableFormat; v != `` {
		queryGen.TableNameFormat = v
	}

	if v := self.queryGenFieldFormat; v != `` {
		queryGen.FieldNameFormat = v
	}

	if v := self.queryGenStringNormalizerFormat; v != `` {
		queryGen.StringNormalizerFormat = v
	}

	return queryGen
}

func (self *SqlBackend) scanFnValueToRecord(collection *dal.Collection, columns []string, scanFn reflect.Value) (*dal.Record, error) {
	if scanFn.Kind() != reflect.Func {
		return nil, fmt.Errorf("Can only accept a function value")
	}

	output := make([]interface{}, len(columns))
	// sql.Row.Scan is strict about how we call it (e.g.: won't return results as a map),
	// so we hack...
	//
	rRowArgs := make([]reflect.Value, len(output))

	// each argument in the call to scan will be the address of the corresponding
	// item in the output array
	for i, _ := range output {
		rRowArgs[i] = reflect.ValueOf(output).Index(i).Addr()
	}

	// perform the call to the Scan() function with the correct number of "arguments"
	rRowResult := scanFn.Call(rRowArgs)

	var err error

	// the function should only return one value, an error
	if len(rRowResult) == 1 {
		v := rRowResult[0].Interface()

		if e, ok := v.(error); ok || v == nil {
			err = e
		} else {
			return nil, fmt.Errorf("row scan call returned invalid type (%T)", v)
		}
	} else {
		return nil, fmt.Errorf("invalid response from row scan call")
	}

	// this is the actual error returned from calling Scan()
	if err == nil {
		var id string
		fields := make(map[string]interface{})

		// for each column in the resultset
		for i, column := range columns {
			var value interface{}

			// convert value types as needed
			switch output[i].(type) {
			case []uint8:
				v := output[i].([]uint8)
				value = string(v[:])
			default:
				value = output[i]
			}

			// set the appropriate field for the dal.Record
			if column == collection.IdentityField {
				id = fmt.Sprintf("%v", value)
			} else {
				fields[column] = value
			}
		}

		return dal.NewRecord(id).SetFields(fields), nil
	} else {
		return nil, err
	}
}

func (self *SqlBackend) refreshAllCollections() error {
	if rows, err := self.db.Query(self.listAllTablesQuery); err == nil {
		defer rows.Close()
		knownTables := make([]string, 0)

		// refresh all tables that come back from the list all tables query
		for rows.Next() {
			var tableName string

			if err := rows.Scan(&tableName); err == nil {
				knownTables = append(knownTables, tableName)

				if err := self.refreshCollection(tableName); err != nil {
					log.Errorf("Error refreshing collection %s: %v", tableName, err)
				}
			} else {
				log.Errorf("Error refreshing collection %s: %v", tableName, err)
			}
		}

		// purge from cache any tables that the list all query didn't return
		self.collectionCacheLock.RLock()
		cachedTables := maputil.StringKeys(self.collectionCache)
		self.collectionCacheLock.RUnlock()

		for _, cached := range cachedTables {
			if !sliceutil.ContainsString(knownTables, cached) {
				self.collectionCacheLock.Lock()
				delete(self.collectionCache, cached)
				self.collectionCacheLock.Unlock()
			}
		}

		return rows.Err()
	} else {
		return err
	}
}

func (self *SqlBackend) refreshCollection(name string) error {
	if collection, err := self.refreshCollectionFunc(
		strings.TrimPrefix(self.conn.Dataset(), `/`),
		name,
	); err == nil {
		if len(collection.Fields) > 0 {
			self.collectionCacheLock.Lock()
			defer self.collectionCacheLock.Unlock()
			self.collectionCache[collection.Name] = collection
		}

		return nil
	} else {
		return err
	}
}

func (self *SqlBackend) getCollectionFromCache(name string) (*dal.Collection, error) {
	self.collectionCacheLock.RLock()
	collection, ok := self.collectionCache[name]
	self.collectionCacheLock.RUnlock()

	if ok {
		return collection, nil
	} else {
		return nil, dal.CollectionNotFound
	}
}
