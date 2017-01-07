package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter/generators"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

func (self *SqlBackend) initializeSqlite() (string, string, error) {
	// tell the backend cool details about generating sqlite-compatible SQL
	self.queryGenTypeMapping = generators.SqliteTypeMapping
	self.queryGenTableFormat = "%q"
	self.queryGenFieldFormat = "%q"
	self.queryGenStringNormalizerFormat = "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(%v, ':', ' '), '[', ' '), ']', ' '), '*', ' '))"
	self.listAllTablesQuery = `SELECT name FROM sqlite_master`
	self.createPrimaryKeyIntFormat = `%s INTEGER NOT NULL PRIMARY KEY ASC`
	self.createPrimaryKeyStrFormat = `%s TEXT NOT NULL PRIMARY KEY`

	// the bespoke method for determining table information for sqlite3
	self.refreshCollectionFunc = func(datasetName string, collectionName string) (*dal.Collection, error) {
		if rows, err := self.db.Query(fmt.Sprintf("PRAGMA table_info(%q)", collectionName)); err == nil {
			defer rows.Close()
			collection := dal.NewCollection(collectionName)

			queryGen := self.makeQueryGen()

			var foundPrimaryKey bool

			for rows.Next() {
				var i, nullable, pk int
				var column, columnType string
				var defaultValue sql.NullString

				if err := rows.Scan(&i, &column, &columnType, &nullable, &defaultValue, &pk); err == nil {
					// start building the dal.Field
					field := dal.Field{
						Name: column,
						Properties: &dal.FieldProperties{
							NativeType:   columnType,
							Required:     (nullable != 1),
							DefaultValue: stringutil.Autotype(defaultValue.String),
						},
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
						field.Type = dal.RawType

					}

					if pk == 1 {
						if !foundPrimaryKey {
							field.Properties.Identity = true
							foundPrimaryKey = true
							collection.IdentityField = column
						} else {
							field.Properties.Key = true
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

	dataset := self.conn.Dataset()
	var dsn string

	switch dataset {
	case `/memory`:
		return `sqlite3`, `:memory:`, nil
	default:
		if strings.HasPrefix(dataset, `/.`) {
			dataset = strings.TrimPrefix(dataset, `/`)
		} else if strings.HasPrefix(dataset, `/~`) {
			dataset = strings.TrimPrefix(dataset, `/`)

			if v, err := pathutil.ExpandUser(dataset); err == nil {
				dataset = v
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
