package main

import (
	"fmt"
	"log"
	"net/mail"
	"os"
	"time"

	"github.com/ghetzel/pivot/v3"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/mapper"
)

type Group struct {
	GID       int       `pivot:"gid,identity"`
	Name      string    `pivot:"name"`
	CreatedAt time.Time `pivot:"created_at"`
	UpdatedAt time.Time `pivot:"updated_at"`
}

type User struct {
	Username  string    `pivot:"id,identity"`
	Email     string    `pivot:"email"`
	Group     *Group    `pivot:"group"`
	CreatedAt time.Time `pivot:"created_at"`
	UpdatedAt time.Time `pivot:"updated_at"`
}

var GroupsTable = &dal.Collection{
	Name:              `groups`,
	IdentityField:     `gid`,
	IdentityFieldType: dal.IntType,
	Fields: []dal.Field{
		{
			Name:        `name`,
			Description: `The display name of the group.`,
			Type:        dal.StringType,
			Required:    true,
		}, {
			Name:         `created_at`,
			Type:         dal.TimeType,
			Required:     true,
			DefaultValue: `now`,

			// set created_at only if it's currently nil
			Formatter: dal.CurrentTimeIfUnset,
		}, {
			Name:         `updated_at`,
			Type:         dal.TimeType,
			Required:     true,
			DefaultValue: `now`,
		},
	},
}

var UsersTable = &dal.Collection{
	Name: `users`,

	// primary key is the username
	IdentityField:     `username`,
	IdentityFieldType: dal.StringType,

	// enforce that usernames be snake_cased and whitespace-trimmed
	IdentityFieldFormatter: dal.FormatAll(
		dal.TrimSpace,
		dal.ChangeCase(`underscore`),
	),

	Fields: []dal.Field{
		{
			Name:     `email`,
			Type:     dal.StringType,
			Required: true,

			// enforces RFC 5322 formatting on email addresses
			Formatter: func(email interface{}, inOrOut dal.FieldOperation) (interface{}, error) {
				if addr, err := mail.ParseAddress(fmt.Sprintf("%v", email)); err == nil {
					return addr.String(), nil
				} else {
					return nil, err
				}
			},
		}, {
			Name:         `group_id`,
			Type:         dal.IntType,
			DefaultValue: true,
			Required:     true,
			BelongsTo:    GroupsTable,
		}, {
			Name:     `created_at`,
			Type:     dal.TimeType,
			Required: true,

			// set created_at only if it's currently nil
			Formatter: dal.CurrentTimeIfUnset,
		}, {
			Name:     `updated_at`,
			Type:     dal.TimeType,
			Required: true,

			// set updated_at to the current time, every time
			Formatter: dal.CurrentTime,
		},
	},
}

func main() {
	var connectionString string

	if len(os.Args) < 2 {
		connectionString = `sqlite:///tmp/.pivot-embedded-collections.db`
	} else {
		connectionString = os.Args[1]
	}

	// setup a connection to the database
	if db, err := pivot.NewDatabase(connectionString); err == nil {
		db = backends.NewEmbeddedRecordBackend(db)

		// make sure we can actually talk to the database
		if err := db.Ping(10 * time.Second); err != nil {
			log.Fatalf("Database connection test failed: %v", err)
		}

		Users := mapper.NewModel(db, UsersTable)
		Groups := mapper.NewModel(db, GroupsTable)

		// creates the table, and fails if the existing schema does not match the
		// UsersTable collection definition above
		if err := Groups.Migrate(); err != nil {
			log.Fatalf("migrating groups table failed: %v", err)
		}

		// creates the table, and fails if the existing schema does not match the
		// UsersTable collection definition above
		if err := Users.Migrate(); err != nil {
			log.Fatalf("migrating users table failed: %v", err)
		}

		// CREATE
		// -----------------------------------------------------------------------------------------

		// make a user object resembling user input
		user := User{
			Username: "  Test\nUser  ",
			Email:    "test.user+testing@example.com",
			Group: &Group{
				GID:  4,
				Name: `Test Group`,
			},
		}

		// create the user
		if err := Users.Create(&user); err != nil {
			log.Fatalf("failed to create user: %v", err)
		}

		var readback User

		// read back the user
		if err := Users.Get(user.Username, &readback); err != nil {
			log.Fatalf("failed to retrieve user: %v", err)
		}

		// print out what we've done
		log.Printf("User %q created email=%q group=%+v", readback.Username, readback.Email, readback.Group)
	} else {
		log.Fatalf("failed to instantiate database: %v", err)
	}
}
