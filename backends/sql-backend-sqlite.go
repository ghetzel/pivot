package backends

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter/generators"
	_ "github.com/mattn/go-sqlite3"
)

func (self *SqlBackend) initializeSqlite() (string, string, error) {
	// tell the backend cool details about generating compatible SQL
	self.queryGenTypeMapping = generators.SqliteTypeMapping
	self.queryGenNormalizerFormat = "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(%v, ':', ' '), '[', ' '), ']', ' '), '*', ' '))"
	self.listAllTablesQuery = `SELECT name FROM sqlite_master`
	self.createPrimaryKeyIntFormat = `%s INTEGER NOT NULL`
	self.createPrimaryKeyStrFormat = `%s TEXT NOT NULL`
	self.foreignKeyConstraintFormat = `FOREIGN KEY(%s) REFERENCES %s(%s) %s`
	self.defaultCurrentTimeString = `CURRENT_TIMESTAMP`

	// the bespoke method for determining table information for sqlite3
	self.refreshCollectionFunc = func(datasetName string, collectionName string) (*dal.Collection, error) {
		var uniqueConstraints []string

		if c, err := self.sqliteGetTableConstraints(`unique`, collectionName); err == nil {
			uniqueConstraints = c
		} else {
			return nil, err
		}

		compileOptions := `PRAGMA compile_options`
		querylog.Debugf("[%T] %s", self, compileOptions)

		if options, err := self.db.Query(compileOptions); err == nil {
			defer options.Close()

			for options.Next() {
				var option string

				if err := options.Scan(&option); err == nil {
					switch option {
					case `ENABLE_JSON1`:
						// self.queryGenNestedFieldFormat = "json_extract(%v, '$.%v')"
						log.Debugf("sqlite: using JSON1 extension")
					}
				} else {
					return nil, err
				}
			}
		} else {
			return nil, err
		}

		stmt := fmt.Sprintf("PRAGMA table_info(%q)", collectionName)
		querylog.Debugf("[%T] %s", self, stmt)

		if rows, err := self.db.Query(stmt); err == nil {
			defer rows.Close()
			collection := dal.NewCollection(collectionName)

			queryGen := self.makeQueryGen(nil)

			var foundPrimaryKey bool

			for rows.Next() {
				var i, required, pk int
				var column, columnType string
				var defaultValue sql.NullString

				if err := rows.Scan(&i, &column, &columnType, &required, &defaultValue, &pk); err == nil {
					// start building the dal.Field
					field := dal.Field{
						Name:       column,
						NativeType: columnType,
						Required:   (required == 1),
						Unique:     sliceutil.ContainsString(uniqueConstraints, column),
					}

					// set default value if it's not NULL
					if defaultValue.Valid {
						field.DefaultValue = stringutil.Autotype(defaultValue.String)
					}

					// tease out type, length, and precision from the native type
					// e.g: DOULBE(8,12) -> "DOUBLE", 8, 12
					columnType, field.Length, field.Precision = queryGen.SplitTypeLength(columnType)

					// map native types to DAL types
					switch columnType {
					case `TEXT`:
						field.Type = dal.StringType

					case `INTEGER`:
						if field.Length == 1 {
							field.Type = dal.BooleanType
						} else {
							field.Type = dal.IntType
						}

					case `REAL`:
						field.Type = dal.FloatType

					default:
						switch field.Length {
						case SqlObjectFieldHintLength:
							field.Type = dal.ObjectType
						case SqlArrayFieldHintLength:
							field.Type = dal.ArrayType
						default:
							field.Type = dal.RawType
						}
					}

					if pk == 1 {
						if !foundPrimaryKey {
							field.Identity = true
							foundPrimaryKey = true
							collection.IdentityField = column
							collection.IdentityFieldType = field.Type
						} else {
							field.Key = true
						}
					}

					// add field to the collection we're building
					collection.Fields = append(collection.Fields, field)
				} else {
					return nil, err
				}
			}

			return collection, rows.Err()
		} else {
			return nil, err
		}
	}

	dataset := path.Join(self.conn.Dataset(), self.conn.Host())

	var dsn string

	switch dataset {
	case `memory`, `:memory:`, ``:
		return `sqlite3`, `:memory:`, nil
	default:
		if strings.HasPrefix(dataset, `~`) {
			if v, err := pathutil.ExpandUser(dataset); err == nil {
				dataset = v
			} else {
				return ``, ``, err
			}
		} else if strings.HasPrefix(dataset, `.`) {
			if v, err := filepath.Abs(dataset); err == nil {
				dataset = v
			} else {
				return ``, ``, err
			}
		}

		switch dataset {
		case `temporary`, `:temporary:`:
			if tmp, err := ioutil.TempFile(``, `pivot-`); err == nil {
				dataset = tmp.Name()
				log.Noticef("[%T] Temporary file: %v", self, dataset)
			} else {
				return ``, ``, err
			}
		}

		dsn = dataset

		opts := make(map[string]interface{})

		if v := self.conn.OptString(`cache`, `shared`); v != `` {
			opts[`cache`] = v
		}

		if v := self.conn.OptString(`mode`, `memory`); v != `` {
			opts[`mode`] = v
		}

		if len(opts) > 0 {
			dsn = dsn + `?` + maputil.Join(opts, `=`, `&`)
		}

		return `sqlite3`, dsn, nil
	}
}

func (self *SqlBackend) sqliteGetTableConstraints(constraintType string, collectionName string) ([]string, error) {
	columns := make([]string, 0)

	stmt := fmt.Sprintf("PRAGMA index_list(%q)", collectionName)
	querylog.Debugf("[%T] %s", self, string(stmt[:]))

	if rows, err := self.db.Query(stmt); err == nil {
		defer rows.Close()

		for rows.Next() {
			var i, isUnique, isPartial int
			var indexName, createdBy string

			if err := rows.Scan(&i, &indexName, &isUnique, &createdBy, &isPartial); err == nil {
				switch constraintType {
				case `unique`:
					if isUnique != 1 {
						continue
					}
				}

				stmt := fmt.Sprintf("PRAGMA index_info(%q)", indexName)
				querylog.Debugf("[%T] %s", self, string(stmt[:]))

				if indexInfo, err := self.db.Query(stmt); err == nil {
					defer indexInfo.Close()

					for indexInfo.Next() {
						var j, columnIndex int
						var columnName string

						if err := indexInfo.Scan(&j, &columnIndex, &columnName); err == nil {
							columns = append(columns, columnName)
						} else {
							return nil, err
						}
					}
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		return columns, nil
	} else {
		return nil, err
	}
}
