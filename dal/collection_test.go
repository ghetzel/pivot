package dal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testGroup struct {
	ID   int    `pivot:"id,identity"`
	Name string `pivot:"name"`
}

type testUser struct {
	ID    int        `pivot:"id,identity"`
	Name  string     `pivot:"name"`
	Group *testGroup `pivot:"group_id"`
	Age   int        `pivot:"age"`
}

type testUserEmbed struct {
	testUser
	Username string `pivot:"username,identity"`
	Active   bool   `pivot:"active"`
}

type nullBackend struct {
	collections map[string]*Collection
}

func (self *nullBackend) RegisterCollection(def *Collection) {
	if self.collections == nil {
		self.collections = make(map[string]*Collection)
	}

	def.SetBackend(self)
	self.collections[def.Name] = def
}

func (self *nullBackend) GetCollection(name string) (*Collection, error) {
	if c, ok := self.collections[name]; ok {
		return c, nil
	} else {
		return nil, CollectionNotFound
	}
}

func TestCollectionStructToRecord(t *testing.T) {
	assert := require.New(t)

	collection := NewCollection(`TestCollectionStructToRecord`)
	collection.AddFields([]Field{
		{
			Name: `name`,
			Type: StringType,
		}, {
			Name: `enabled`,
			Type: BooleanType,
		}, {
			Name: `age`,
			Type: IntType,
		},
	}...)

	type TestRecord struct {
		Name      string `pivot:"name"`
		Enabled   bool   `pivot:"enabled,omitempty"`
		ActualAge int    `pivot:"age"`
		Age       int
	}

	testRecord := TestRecord{
		Name:      `tester`,
		ActualAge: 42,
	}

	record, err := collection.StructToRecord(&testRecord)
	assert.NoError(err)
	assert.NotNil(record)

	assert.Nil(record.ID)
	assert.EqualValues(`tester`, record.Get(`name`))
	assert.EqualValues(42, record.Get(`age`))

	type TestRecord2 struct {
		ID        int
		Name      string `pivot:"name"`
		Enabled   bool   `pivot:"enabled,omitempty"`
		ActualAge int    `pivot:"age"`
		Age       int
	}

	testRecord2 := TestRecord2{
		ID:   11,
		Name: `tester`,
	}

	record, err = collection.StructToRecord(&testRecord2)
	assert.NoError(err)
	assert.NotNil(record)

	assert.Equal(3, len(record.Fields))

	assert.Equal(11, record.ID)
	assert.Equal(`tester`, record.Get(`name`))
	assert.Equal(false, record.Get(`enabled`))
	assert.EqualValues(0, record.Get(`age`))
}

func TestCollectionStructToRecordRelated(t *testing.T) {
	assert := require.New(t)

	groups := NewCollection(`TestCollectionStructToRecordGroups`, Field{
		Name: `name`,
		Type: StringType,
	})

	users := NewCollection(`TestCollectionStructToRecordUsers`, Field{
		Name: `name`,
		Type: StringType,
	}, Field{
		Name:      `group_id`,
		Type:      IntType,
		BelongsTo: groups,
	}, Field{
		Name: `age`,
		Type: IntType,
	})

	backend := new(nullBackend)
	backend.RegisterCollection(users)
	backend.RegisterCollection(groups)

	record, err := users.StructToRecord(&testUser{
		Name: `tester`,
		Group: &testGroup{
			ID: 5432,
		},
		Age: 42,
	})
	assert.NoError(err)
	assert.NotNil(record)

	assert.Zero(record.ID)
	assert.Equal(`tester`, record.Get(`name`))
	assert.EqualValues(5432, record.Get(`group_id`))
	assert.EqualValues(42, record.Get(`age`))
}

func TestCollectionValidator(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name:              `TestCollectionValidator`,
		IdentityFieldType: StringType,
		PreSaveValidator: func(record *Record) error {
			if fmt.Sprintf("%v", record.ID) == `two` {
				return fmt.Errorf("ID cannot be 'two' for reasons")
			}

			return nil
		},
	}

	assert.NoError(collection.ValidateRecord(NewRecord(`one`), PersistOperation))
	assert.Error(collection.ValidateRecord(NewRecord(`two`), PersistOperation))
	assert.NoError(collection.ValidateRecord(NewRecord(`three`), PersistOperation))
}

func TestCollectionMapFromRecord(t *testing.T) {
	assert := require.New(t)

	collection := NewCollection(`TestCollectionStructToRecord`)
	collection.IdentityFieldFormatter = func(id interface{}, op FieldOperation) (interface{}, error) {
		if record, ok := id.(*Record); ok {
			id = record.ID
		}

		return fmt.Sprintf("ID<%v>", id), nil
	}

	collection.AddFields([]Field{
		{
			Name: `name`,
			Type: StringType,
		}, {
			Name:         `enabled`,
			Type:         BooleanType,
			DefaultValue: true,
		}, {
			Name: `age`,
			Type: IntType,
		},
	}...)

	rv, err := collection.MapFromRecord(
		NewRecord(`test`).Set(`name`, `tester`).Set(`age`, 42),
	)

	assert.NoError(err)
	assert.EqualValues(map[string]interface{}{
		`id`:      `ID<test>`,
		`name`:    `tester`,
		`enabled`: true,
		`age`:     42,
	}, rv)
}

func TestCollectionTTLField(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name:              `TestCollectionTTLField`,
		IdentityFieldType: StringType,
		TimeToLiveField:   `ttl`,
	}

	assert.Zero(collection.TTL(NewRecord(`test1`)))
	assert.Zero(collection.TTL(NewRecord(`test1`).Set(`ttl`, nil)))
	assert.Zero(collection.TTL(NewRecord(`test1`).Set(`ttl`, 0)))
	assert.Zero(collection.TTL(NewRecord(`test1`).Set(`ttl`, ``)))
	assert.Zero(collection.TTL(NewRecord(`test1`).Set(`ttl`, false)))

	assert.NotZero(
		collection.TTL(NewRecord(`test1`).Set(`ttl`, time.Now().Add(time.Second))),
	)

	assert.NotZero(
		collection.TTL(NewRecord(`test1`).Set(`ttl`, time.Now().Unix()+60)),
	)
}

func TestCollectionIsExpired(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name:              `TestCollectionIsExpired`,
		IdentityFieldType: StringType,
		TimeToLiveField:   `ttl`,
	}

	assert.False(collection.IsExpired(NewRecord(`test1`)))
	assert.False(collection.IsExpired(NewRecord(`test1`).Set(`ttl`, nil)))
	assert.False(collection.IsExpired(NewRecord(`test1`).Set(`ttl`, 0)))
	assert.False(collection.IsExpired(NewRecord(`test1`).Set(`ttl`, ``)))
	assert.False(collection.IsExpired(NewRecord(`test1`).Set(`ttl`, false)))

	assert.False(
		collection.IsExpired(NewRecord(`test1`).Set(`ttl`, time.Now().Add(time.Second))),
	)

	assert.False(
		collection.IsExpired(NewRecord(`test1`).Set(`ttl`, time.Now().Unix()+60)),
	)

	assert.True(
		collection.IsExpired(NewRecord(`test1`).Set(`ttl`, time.Now().Add(-1*time.Nanosecond))),
	)

	assert.True(
		collection.IsExpired(NewRecord(`test1`).Set(`ttl`, time.Now().Unix()-60)),
	)
}

func TestCollectionExtractValueFromRelationship(t *testing.T) {
	assert := require.New(t)

	groups := NewCollection(`TestCollectionExtractValueFromRelationshipGroups`, Field{
		Name: `name`,
		Type: StringType,
	})

	users := NewCollection(`TestCollectionExtractValueFromRelationshipUsers`, Field{
		Name: `name`,
		Type: StringType,
	}, Field{
		Name:      `group_id`,
		Type:      IntType,
		BelongsTo: groups,
	}, Field{
		Name: `age`,
		Type: IntType,
	})

	backend := new(nullBackend)
	backend.RegisterCollection(users)
	backend.RegisterCollection(groups)

	// test ability for Users to extract key values from Groups
	f, ok := users.GetField(`group_id`)
	assert.True(ok)
	keys, err := users.extractValueFromRelationship(&f, &testGroup{
		ID: 5432,
	}, PersistOperation)

	assert.NoError(err)
	assert.EqualValues(5432, keys)

}
