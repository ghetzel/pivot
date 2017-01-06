package backends

import (
	"database/sql"
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

func (self *SqlBackend) initializeMysql() (string, string, error) {
	databaseName := strings.TrimPrefix(self.conn.Dataset(), `/`)

	// tell the backend cool details about generating sqlite-compatible SQL
	self.queryGenTypeMapping = generators.MysqlTypeMapping
	self.queryGenPlaceholderFormat = `?`
	self.queryGenPlaceholderArgument = ``
	self.queryGenTableFormat = "`%s`"
	self.queryGenFieldFormat = "`%s`"
	self.listAllTablesQuery = `SHOW TABLES`
	self.createPrimaryKeyIntFormat = `%s INT AUTO_INCREMENT NOT NULL PRIMARY KEY`
	self.createPrimaryKeyStrFormat = `%s VARCHAR(255) NOT NULL PRIMARY KEY`

	// the bespoke method for determining table information for sqlite3
	self.tableDetailsFunc = func(collectionName string, fieldFn sqlAddFieldFunc) error {
		if f, err := filter.FromMap(map[string]interface{}{
			`TABLE_SCHEMA`: databaseName,
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

			if sqlString, err := filter.Render(queryGen, "`information_schema`.`COLUMNS`", f); err == nil {
				if rows, err := self.db.Query(string(sqlString[:]), queryGen.GetValues()...); err == nil {
					defer rows.Close()

					for rows.Next() {
						var i int
						var column, columnType, nullable string
						var defaultValue, keyType sql.NullString

						if err := rows.Scan(&i, &column, &columnType, &nullable, &defaultValue, &keyType); err == nil {
							details := sqlTableDetails{
								Index:        i,
								Name:         column,
								NativeType:   columnType,
								Nullable:     (nullable == `YES`), // thanks, MySQL ;)
								DefaultValue: defaultValue.String,
							}

							columnType, details.TypeLength, details.Precision = queryGen.SplitTypeLength(columnType)

							if strings.HasSuffix(columnType, `CHAR`) || strings.HasSuffix(columnType, `TEXT`) {
								details.Type = `str`

							} else if strings.HasPrefix(columnType, `BOOL`) || columnType == `BIT` {
								details.Type = `bool`

							} else if strings.HasSuffix(columnType, `INT`) {
								if details.TypeLength == 1 {
									details.Type = `bool`
								} else {
									details.Type = `int`
								}

							} else if columnType == `FLOAT` || columnType == `DOUBLE` || columnType == `DECIMAL` {
								details.Type = `float`

							} else if strings.HasPrefix(columnType, `DATE`) || strings.Contains(columnType, `TIME`) {
								details.Type = `date`

							} else {
								details.Type = stringutil.Underscore(columnType)
							}

							switch keyType.String {
							case `PRI`:
								details.PrimaryKey = true
							case `UNI`:
								details.Unique = true
							case `MUL`:
								details.KeyField = true
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
			} else {
				return err
			}
		} else {
			return err
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
