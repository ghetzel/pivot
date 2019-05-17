package backends

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
	_ "github.com/lib/pq"
)

func (self *SqlBackend) initializePostgres() (string, string, error) {
	// tell the backend cool details about generating compatible SQL
	self.queryGenTypeMapping = generators.PostgresTypeMapping
	self.queryGenNormalizerFormat = "regexp_replace(lower(%v), '[\\:\\[\\]\\*]+', ' ')"
	self.listAllTablesQuery = `SELECT table_name from information_schema.TABLES WHERE table_catalog = CURRENT_CATALOG AND table_schema = 'public'`
	self.createPrimaryKeyIntFormat = `%s BIGSERIAL`
	self.createPrimaryKeyStrFormat = `%s VARCHAR(255)`
	self.countEstimateQuery = "SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = to_regclass('%s')"
	self.countExactQuery = "SELECT COUNT(*) AS exact FROM (SELECT 1 FROM %s LIMIT %d) t"
	self.foreignKeyConstraintFormat = `FOREIGN KEY(%s) REFERENCES %s (%s) %s`
	// self.defaultCurrentTimeString = `now() AT TIME ZONE 'utc'`
	self.defaultCurrentTimeString = `CURRENT_TIMESTAMP`

	// the bespoke method for determining table information for sqlite3
	self.refreshCollectionFunc = func(datasetName string, collectionName string) (*dal.Collection, error) {
		keyStmt := `SELECT ` +
			`kc.column_name, tc.constraint_type ` +
			`FROM information_schema.table_constraints tc, information_schema.key_column_usage kc ` +
			`WHERE kc.table_name = tc.table_name ` +
			`AND kc.table_schema = tc.table_schema ` +
			`AND kc.constraint_name = tc.constraint_name ` +
			`AND tc.constraint_catalog = CURRENT_CATALOG ` +
			`AND tc.table_name = $1 ` +
			`ORDER BY kc.column_name, tc.constraint_type`

		primaryKeys := make(map[string]bool)
		uniqueKeys := make(map[string]bool)
		foreignKeys := make(map[string]bool)

		if keyRows, err := self.db.Query(string(keyStmt[:]), collectionName); err == nil {
			defer keyRows.Close()

			// for each key on this table...
			for keyRows.Next() {
				var columnName, constraintType string

				if err := keyRows.Scan(&columnName, &constraintType); err == nil {
					switch constraintType {
					case `PRIMARY KEY`:
						primaryKeys[columnName] = true
					case `FOREIGN KEY`:
						foreignKeys[columnName] = true
					case `UNIQUE`:
						uniqueKeys[columnName] = true
					}
				} else {
					return nil, err
				}
			}

			keyRows.Close()
		} else {
			return nil, err
		}

		if f, err := filter.FromMap(map[string]interface{}{
			`table_catalog`: datasetName,
			`table_name`:    collectionName,
			`table_schema`:  `public`,
		}); err == nil {
			f.Fields = []string{
				`ordinal_position`,
				`column_name`,
				`data_type`,
				`character_octet_length`,
				`is_nullable`,
				`column_default`,
			}

			queryGen := self.makeQueryGen(nil)

			// make this instance of the query generator use the table name as given because
			// we need to reference another database (information_schema)
			queryGen.TypeMapping.TableNameFormat = "%s"

			if stmt, err := filter.Render(queryGen, `information_schema.COLUMNS`, f); err == nil {
				querylog.Debugf("[%T] %s %v", self, string(stmt[:]), queryGen.GetValues())

				if rows, err := self.db.Query(string(stmt[:]), queryGen.GetValues()...); err == nil {
					defer rows.Close()

					collection := dal.NewCollection(collectionName)

					// for each field in the schema description for this table...
					for rows.Next() {
						var i int
						var octetLength sql.NullInt64
						var column, columnType, nullable string
						var defaultValue sql.NullString

						// populate variables from column values
						if err := rows.Scan(&i, &column, &columnType, &octetLength, &nullable, &defaultValue); err == nil {
							// start building the dal.Field
							field := dal.Field{
								Name:       column,
								NativeType: columnType,
								Required:   (nullable != `YES`),
							}

							// set default value if it's not NULL
							if defaultValue.Valid && !stringutil.IsSurroundedBy(defaultValue.String, `nextval(`, `)`) {
								field.DefaultValue = stringutil.Autotype(defaultValue.String)
							}

							// tease out type, length, and precision from the native type
							// e.g: DOULBE(8,12) -> "DOUBLE", 8, 12
							columnType = strings.ToUpper(columnType)
							field.Length = int(octetLength.Int64)
							// field.Precision =

							// map native types to DAL types
							if strings.Contains(columnType, `CHAR`) || strings.HasSuffix(columnType, `TEXT`) {
								field.Type = dal.StringType

							} else if strings.HasPrefix(columnType, `BOOL`) {
								field.Type = dal.BooleanType

							} else if strings.HasPrefix(columnType, `INT`) || strings.HasSuffix(columnType, `INT`) {
								if field.Length == 1 {
									field.Type = dal.BooleanType
								} else {
									field.Type = dal.IntType
								}

							} else if columnType == `NUMERIC` || columnType == `REAL` || columnType == `FLOAT` || strings.HasPrefix(columnType, `DOUBLE`) {
								field.Type = dal.FloatType

							} else if strings.HasPrefix(columnType, `DATE`) || strings.Contains(columnType, `TIME`) {
								field.Type = dal.TimeType

							} else {
								switch field.Length {
								case SqlObjectFieldHintLength:
									field.Type = dal.ObjectType
								case SqlArrayFieldHintLength:
									field.Type = dal.ArrayType
								default:
									field.Type = dal.RawType
								}
							}

							// figure out keying
							if v, ok := primaryKeys[column]; ok && v {
								field.Identity = true
								collection.IdentityField = column
								collection.IdentityFieldType = field.Type
							} else if v, ok := foreignKeys[column]; ok && v {
								field.Key = true
							}

							if v, ok := uniqueKeys[column]; ok && v {
								field.Unique = true
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
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	var dsn, host string

	dsn = `postgres://`

	// prepend port to host if not present
	if strings.Contains(self.conn.Host(), `:`) {
		host = self.conn.Host()
	} else {
		host = fmt.Sprintf("%s:5432", self.conn.Host())
	}

	if u, p, ok := self.conn.Credentials(); ok {
		dsn += fmt.Sprintf("%s:%s@", u, p)
	}

	dsn += host
	dsn += `/` + self.conn.Dataset()

	opts := self.conn.URI.Query()

	// pull out pivot-specific options first
	for k, vv := range opts {
		switch k {
		case `autoregister`:
			self.conn.Options[k] = strings.Join(vv, `,`)
			opts.Del(k)
		case `autocount`:
			self.conn.Options[k] = typeutil.V(vv).Bool()
			opts.Del(k)
		}
	}

	opts.Set(`sslmode`, sliceutil.OrString(opts.Get(`sslmode`), `disable`))

	if v := opts.Encode(); v != `` {
		dsn += `?` + v
	}

	return `postgres`, dsn, nil
}
