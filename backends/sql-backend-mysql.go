package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

func (self *SqlBackend) initializeMysql() (string, string, error) {
	// tell the backend cool details about generating sqlite-compatible SQL
	self.queryGenTypeMapping = generators.MysqlTypeMapping
	self.queryGenPlaceholderFormat = `?`
	self.queryGenPlaceholderArgument = ``
	self.queryGenTableFormat = "`%s`"
	self.queryGenFieldFormat = "`%s`"
	self.queryGenStringNormalizerFormat = "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(%v, ':', ' '), '[', ' '), ']', ' '), '*', ' '))"
	self.listAllTablesQuery = `SHOW TABLES`
	self.createPrimaryKeyIntFormat = `%s INT AUTO_INCREMENT NOT NULL PRIMARY KEY`
	self.createPrimaryKeyStrFormat = `%s VARCHAR(255) NOT NULL PRIMARY KEY`

	// the bespoke method for determining table information for sqlite3
	self.refreshCollectionFunc = func(datasetName string, collectionName string) (*dal.Collection, error) {
		if f, err := filter.FromMap(map[string]interface{}{
			`TABLE_SCHEMA`: datasetName,
			`TABLE_NAME`:   collectionName,
		}); err == nil {
			f.Fields = []string{
				`ORDINAL_POSITION`,
				`COLUMN_NAME`,
				`DATA_TYPE`,
				`IS_NULLABLE`,
				`COLUMN_DEFAULT`,
				`COLUMN_KEY`,
			}

			queryGen := self.makeQueryGen()

			// make this instance of the query generator use the table name as given because
			// we need to reference another database (information_schema)
			queryGen.TableNameFormat = "%s"
			queryGen.StringNormalizerFormat = "%s"

			if sqlString, err := filter.Render(queryGen, "`information_schema`.`COLUMNS`", f); err == nil {
				if rows, err := self.db.Query(string(sqlString[:]), queryGen.GetValues()...); err == nil {
					defer rows.Close()

					collection := dal.NewCollection(collectionName)

					// for each field in the schema description for this table...
					for rows.Next() {
						var i int
						var column, columnType, nullable string
						var defaultValue, keyType sql.NullString

						// populate variables from column values
						if err := rows.Scan(&i, &column, &columnType, &nullable, &defaultValue, &keyType); err == nil {
							// start building the dal.Field
							field := dal.Field{
								Name: column,
								Properties: &dal.FieldProperties{
									NativeType:   columnType,
									Required:     (nullable != `YES`),
									DefaultValue: stringutil.Autotype(defaultValue.String),
								},
							}

							// tease out type, length, and precision from the native type
							// e.g: DOULBE(8,12) -> "DOUBLE", 8, 12
							columnType, field.Length, field.Precision = queryGen.SplitTypeLength(columnType)

							// map native types to DAL types
							if strings.HasSuffix(columnType, `CHAR`) || strings.HasSuffix(columnType, `TEXT`) {
								field.Type = `str`

							} else if strings.HasPrefix(columnType, `BOOL`) || columnType == `BIT` {
								field.Type = `bool`

							} else if strings.HasSuffix(columnType, `INT`) {
								if field.Length == 1 {
									field.Type = `bool`
								} else {
									field.Type = `int`
								}

							} else if columnType == `FLOAT` || columnType == `DOUBLE` || columnType == `DECIMAL` {
								field.Type = `float`

							} else if strings.HasPrefix(columnType, `DATE`) || strings.Contains(columnType, `TIME`) {
								field.Type = `date`

							} else {
								field.Type = stringutil.Underscore(columnType)
							}

							// figure out keying
							switch keyType.String {
							case `PRI`:
								field.Properties.Identity = true
								collection.IdentityField = column
							case `UNI`:
								field.Properties.Unique = true
							case `MUL`:
								field.Properties.Key = true
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

	var dsn, protocol, host string

	// set or autodetect protocol
	if v := self.conn.Protocol(); v != `` {
		protocol = v
	} else if strings.HasPrefix(self.conn.Host(), `/`) {
		protocol = `unix`
	} else {
		protocol = `tcp`
	}

	// prepend port to host if not present
	if strings.Contains(self.conn.Host(), `:`) {
		host = self.conn.Host()
	} else {
		host = fmt.Sprintf("%s:3306", self.conn.Host())
	}

	if up := self.conn.URI.User; up != nil {
		dsn += up.String() + `@`
	}

	dsn += fmt.Sprintf(
		"%s(%s)%s",
		protocol,
		host,
		self.conn.Dataset(),
	)

	return `mysql`, dsn, nil
}
