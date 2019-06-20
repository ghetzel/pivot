package main

import (
	"os"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/pivot/v3"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
)

type Contact struct {
	ID        int64  `pivot:"id,identity"`
	FirstName string `pivot:"first_name"`
	LastName  string `pivot:"last_name"`
	Address   string `pivot:"address"`
	City      string `pivot:"city"`
	State     string `pivot:"state"`
	Country   string `pivot:"country"`
}

type Item struct {
	ID          int     `pivot:"id,identity"`
	Name        string  `pivot:"name"`
	Description string  `pivot:"description"`
	Cost        float64 `pivot:"cost"`
	Currency    string  `pivot:"currency"`
}

type Order struct {
	ID              string    `pivot:"id,identity"`
	Status          string    `pivot:"status"`
	Items           []Item    `pivot:"-"`
	ShippingAddress Contact   `pivot:"shipping_address"`
	BillingAddress  Contact   `pivot:"billing_address"`
	CreatedAt       time.Time `pivot:"created_at"`
	UpdatedAt       time.Time `pivot:"updated_at"`
}

var ItemsTable = &dal.Collection{
	Name: `items`,
	Fields: []dal.Field{
		{
			Name:     `name`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `description`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `cost`,
			Type:     dal.FloatType,
			Required: true,
		}, {
			Name:     `currency`,
			Type:     dal.StringType,
			Required: true,
		},
	},
}

var OrdersTable = &dal.Collection{
	Name: `items`,
	Fields: []dal.Field{
		{
			Name:         `status`,
			Type:         dal.StringType,
			Required:     true,
			DefaultValue: `pending`,
			Validator: dal.ValidateIsOneOf(
				`pending`,
				`received`,
				`processing`,
				`shipped`,
				`delivered`,
				`canceled`,
			),
		}, {
			Name:     `shipping_address`,
			Type:     dal.IntType,
			Required: true,
		}, {
			Name:     `billing_address`,
			Type:     dal.IntType,
			Required: true,
		}, {
			Name:     `currency`,
			Type:     dal.StringType,
			Required: true,
		},
	},
}

var OrdersItemsTable = &dal.Collection{
	Fields: []dal.Field{
		{
			Name:      `order_id`,
			Type:      dal.StringType,
			Required:  true,
			BelongsTo: OrdersTable,
		}, {
			Name:      `item_id`,
			Type:      dal.IntType,
			Required:  true,
			BelongsTo: ItemsTable,
		},
	},
}

var ContactsTable = &dal.Collection{
	Name: `contacts`,
	Fields: []dal.Field{
		{
			Name:     `first_name`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name: `last_name`,
			Type: dal.StringType,
		}, {
			Name: `address`,
			Type: dal.StringType,
		}, {
			Name:     `city`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `state`,
			Type:     dal.StringType,
			Required: true,
		}, {
			Name:     `country`,
			Type:     dal.StringType,
			Required: true,
		},
	},
}

var Contacts pivot.Model
var Items pivot.Model
var Orders pivot.Model
var OrdersItems pivot.Model

func main() {
	var connectionString string

	if len(os.Args) < 2 {
		connectionString = `sqlite:///tmp/.pivot-embedded-collections.db`
	} else {
		connectionString = os.Args[1]
	}

	// setup a connection to the database
	if db, err := pivot.NewDatabase(connectionString); err == nil {
		db.SetBackend(backends.NewEmbeddedRecordBackend(db.GetBackend()))

		// make sure we can actually talk to the database
		if err := db.Ping(10 * time.Second); err != nil {
			log.Fatalf("Database connection test failed: %v", err)
		}

		Contacts = db.AttachCollection(ContactsTable)
		Items = db.AttachCollection(ItemsTable)
		Orders = db.AttachCollection(OrdersTable)
		OrdersItems = db.AttachCollection(OrdersItemsTable)

		// create tables as necessary
		log.FatalfIf("migrate failed: %v", db.Migrate())

		// load data into tables
		log.FatalfIf("load fixtures failed: %v", db.LoadFixtures(`./examples/embedded-collections/fixtures/*.json`))

	} else {
		log.Fatalf("failed to instantiate database: %v", err)
	}
}
