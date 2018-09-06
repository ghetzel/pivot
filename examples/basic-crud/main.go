package main

import (
	"fmt"
	"log"
	"net/mail"
	"os"
	"time"

	"github.com/ghetzel/pivot"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/mapper"
)

type User struct {
	Username  string    `pivot:"id,identity"`
	Email     string    `pivot:"email"`
	Enabled   bool      `pivot:"enabled"`
	CreatedAt time.Time `pivot:"created_at"`
	UpdatedAt time.Time `pivot:"updated_at"`
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
			Name:         `enabled`,
			Type:         dal.BooleanType,
			DefaultValue: true,
			Required:     true,
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
		connectionString = `sqlite:///tmp/.pivot-basic-crud.db`
	} else {
		connectionString = os.Args[1]
	}

	// setup a connection to the database
	if db, err := pivot.NewDatabase(connectionString); err == nil {
		// make sure we can actually talk to the database
		if err := db.Ping(10 * time.Second); err != nil {
			log.Fatalf("Database connection test failed: %v", err)
		}

		Users := mapper.NewModel(db, UsersTable)

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
		}

		// create the user
		if err := Users.Create(&user); err != nil {
			log.Fatalf("failed to create user: %v", err)
		}

		// print out what we've done
		log.Printf("User %q created email=%q", user.Username, user.Email)

		// UPDATES
		// -----------------------------------------------------------------------------------------

		// update the user's email
		user.Email = "other.user@example.com"

		if err := Users.Update(&user); err != nil {
			log.Fatalf("failed to update user: %v", err)
		}

		// RETRIEVAL
		// -----------------------------------------------------------------------------------------

		// read back the user
		if err := Users.Get(user.Username, &user); err != nil {
			log.Fatalf("failed to retrieve user: %v", err)
		}

		log.Printf("User %q now has email=%q", user.Username, user.Email)

		// DELETE
		// -----------------------------------------------------------------------------------------

		// delete the user
		if err := Users.Delete(user.Username); err != nil {
			log.Fatalf("failed to delete user: %v", err)
		}

		// EXISTS
		// -----------------------------------------------------------------------------------------

		// make SURE we deleted the user
		if Users.Exists(user.Username) {
			log.Fatalf("user %q still exists", user.Username)
		} else {
			log.Println("OK")
		}

	} else {
		log.Fatalf("failed to instantiate database: %v", err)
	}
}
