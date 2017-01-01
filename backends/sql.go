package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"reflect"
)

type SqlBackend struct {
	Backend
	conn                        dal.ConnectionString
	db                          *sql.DB
	queryGenTypeMapping         generators.SqlTypeMapping
	queryGenPlaceholderArgument string
	queryGenPlaceholderFormat   string
}

func NewSqlBackend(connection dal.ConnectionString) *SqlBackend {
	return &SqlBackend{
		conn:                connection,
		queryGenTypeMapping: generators.DefaultSqlTypeMapping,
	}
}

func (self *SqlBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

// sqlite3:///

func (self *SqlBackend) Initialize() error {
	backend := self.conn.Backend()
	internalBackend := backend

	var name string
	var dsn string
	var err error

	switch backend {
	case `sqlite`:
		name, dsn, err = self.initializeSqlite()
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

	return nil
}

func (self *SqlBackend) Insert(collection string, recordset *dal.RecordSet) error {
	if tx, err := self.db.Begin(); err == nil {
		for _, record := range recordset.Records {
			queryGen := self.makeQueryGen()
			queryGen.Type = generators.SqlInsertStatement

			for k, v := range record.Fields {
				queryGen.InputData[k] = v
			}

			if record.ID != `` {
				queryGen.InputData[dal.DefaultIdentityField] = record.ID
			}

			if stmt, err := filter.Render(queryGen, collection, filter.Null); err == nil {
				if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err == nil {
					return tx.Commit()
				} else {
					return err
				}
			} else {
				return err
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *SqlBackend) Exists(collection string, id string) bool {
	return false
}

func (self *SqlBackend) Retrieve(collection string, id string, fields ...string) (*dal.Record, error) {
	if f, err := filter.FromMap(map[string]interface{}{
		dal.DefaultIdentityField: id,
	}); err == nil {
		f.Fields = fields
		queryGen := self.makeQueryGen()

		if err := queryGen.Initialize(collection); err == nil {
			if sqlString, err := filter.Render(queryGen, collection, f); err == nil {
				values := queryGen.GetValues()

				log.Debugf("%s %+v", string(sqlString[:]), values)

				// perform query
				if rows, err := self.db.Query(string(sqlString[:]), values...); err == nil {
					defer rows.Close()

					if columns, err := rows.Columns(); err == nil {
						if rows.Next() {
							return self.scanFnValueToRecord(columns, reflect.ValueOf(rows.Scan))
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
}

func (self *SqlBackend) Update(collection string, recordset *dal.RecordSet) error {
	return fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) Delete(collection string, ids []string) error {
	return fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) WithSearch() Indexer {
	return nil
}

func (self *SqlBackend) CreateCollection(definition dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) DeleteCollection(collection string) error {
	return fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) GetCollection(name string) (dal.Collection, error) {
	c := dal.NewCollection(name)
	return *c, fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) makeQueryGen() *generators.Sql {
	queryGen := generators.NewSqlGenerator()
	queryGen.UsePlaceholders = true
	queryGen.TypeMapping = self.queryGenTypeMapping

	if v := self.queryGenPlaceholderFormat; v != `` {
		queryGen.PlaceholderFormat = v
	}

	if v := self.queryGenPlaceholderArgument; v != `` {
		queryGen.PlaceholderArgument = v
	}

	return queryGen
}

func (self *SqlBackend) scanFnValueToRecord(columns []string, scanFn reflect.Value) (*dal.Record, error) {
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

		for i, column := range columns {
			var value interface{}

			switch output[i].(type) {
			case []uint8:
				v := output[i].([]uint8)
				value = string(v[:])
			default:
				value = output[i]
			}

			if column == dal.DefaultIdentityField {
				id = fmt.Sprintf("%v", value)
			} else {
				fields[column] = value
			}
		}

		if id != `` {
			return dal.NewRecord(id).SetFields(fields), nil
		} else {
			return nil, fmt.Errorf("Record ID missing from result set")
		}
	} else {
		return nil, err
	}
}
