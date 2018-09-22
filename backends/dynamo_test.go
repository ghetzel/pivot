package backends

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/stretchr/testify/require"
)

var dynamoTestCollection = &dal.Collection{
	Name: `TestDynamoRecordToItem`,
	Fields: []dal.Field{
		{
			Name: `name`,
			Type: dal.StringType,
		}, {
			Name: `enabled`,
			Type: dal.BooleanType,
		}, {
			Name: `age`,
			Type: dal.IntType,
		},
	},
}

func TestDynamoRecordToItem(t *testing.T) {
	assert := require.New(t)
	attr, err := dynamoRecordToItem(
		dynamoTestCollection,
		dal.NewRecord(123).Set(`name`, `tester`).Set(`age`, 42),
	)

	assert.NoError(err)
	assert.Equal(map[string]*dynamodb.AttributeValue{
		`id`: {
			N: aws.String(`123`),
		},
		`name`: {
			S: aws.String(`tester`),
		},
		`age`: {
			N: aws.String(`42`),
		},
	}, attr)
}

func TestDynamoRecordFromItem(t *testing.T) {
	assert := require.New(t)
	record, err := dynamoRecordFromItem(
		dynamoTestCollection,
		map[string]*dynamodb.AttributeValue{
			`id`: {
				N: aws.String(`123`),
			},
			`name`: {
				S: aws.String(`tester`),
			},
			`age`: {
				N: aws.String(`42`),
			},
		},
	)

	assert.NoError(err)
	assert.EqualValues(123, record.ID)
	assert.EqualValues(`tester`, record.Get(`name`))
	assert.EqualValues(42, record.Get(`age`))
}
