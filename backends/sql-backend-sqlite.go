package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter/generators"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

func (self *SqlBackend) initializeSqlite() (string, string, error) {
	// tell the backend cool details about generating sqlite-compatible SQL
	self.queryGenTypeMapping = generators.SqliteTypeMapping
	self.queryGenTableFormat = "%q"
	self.queryGenFieldFormat = "%q"
	self.listAllTablesQuery = `SELECT name FROM sqlite_master`
	self.createPrimaryKeyFormat = `%s INTEGER NOT NULL PRIMARY KEY ASC`

	// the bespoke method for determining table information for sqlite3
	self.tableDetailsFunc = func(collectionName string, fieldFn sqlAddFieldFunc) error {
		if rows, err := self.db.Query(fmt.Sprintf("PRAGMA table_info(%q)", collectionName)); err == nil {
			defer rows.Close()
			queryGen := self.makeQueryGen()

			var foundPrimaryKey bool

			for rows.Next() {
				var i, nullable, pk int
				var column, columnType string
				var defaultValue sql.NullString

				if err := rows.Scan(&i, &column, &columnType, &nullable, &defaultValue, &pk); err == nil {
					details := sqlTableDetails{
						Index:        i,
						Name:         column,
						NativeType:   columnType,
						Nullable:     (nullable == 1),
						DefaultValue: defaultValue.String,
					}

					columnType, details.TypeLength, details.Precision = queryGen.SplitTypeLength(columnType)

					switch columnType {
					case `TEXT`:
						details.Type = `str`

					case `INTEGER`:
						if details.TypeLength == 1 {
							details.Type = `bool`
						} else {
							details.Type = `int`
						}

					case `REAL`:
						details.Type = `float`

					default:
						details.Type = stringutil.Underscore(columnType)

					}

					if pk == 1 {
						if !foundPrimaryKey {
							details.PrimaryKey = true
							foundPrimaryKey = true
						} else {
							details.KeyField = true
						}
					}

					if err := fieldFn(details); err != nil {
						return err
					}

				} else {
					return err
				}
			}

			return rows.Err()
		} else {
			return err
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
