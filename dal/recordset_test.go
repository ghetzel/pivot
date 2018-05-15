package dal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var deftime = time.Date(2006, 1, 2, 15, 4, 5, 999999, time.FixedZone(`MST`, -420))
var othtime = time.Date(2015, 1, 2, 5, 4, 3, 0, time.UTC)

type testRecordSetRecordDest struct {
	ID        int       `pivot:"id,identity"`
	Name      string    `pivot:"name"`
	Factor    float64   `pivot:"factor"`
	CreatedAt time.Time `pivot:"created_at"`
}

func TestRecordSet(t *testing.T) {
	assert := require.New(t)

	demo := &Collection{
		Fields: []Field{
			{
				Name: `name`,
				Type: StringType,
			}, {
				Name:         `factor`,
				Type:         FloatType,
				Required:     true,
				DefaultValue: 0.2,
			}, {
				Name:     `created_at`,
				Type:     TimeType,
				Required: true,
				DefaultValue: func() interface{} {
					return deftime
				},
			},
		},
	}

	recordset := NewRecordSet(
		NewRecord(1).SetFields(map[string]interface{}{
			`name`:       `First`,
			`factor`:     0.1,
			`created_at`: nil,
		}),
		NewRecord(3).SetFields(map[string]interface{}{
			`name`:       `Second`,
			`factor`:     nil,
			`created_at`: othtime,
		}),
		NewRecord(5).SetFields(map[string]interface{}{
			`name`:       `Third`,
			`factor`:     0.3,
			`created_at`: nil,
		}),
	)

	dest := make([]*testRecordSetRecordDest, 0)
	assert.NoError(recordset.PopulateFromRecords(&dest, demo))
	assert.Len(dest, 3)

	assert.Equal(1, dest[0].ID)
	assert.Equal(`First`, dest[0].Name)
	assert.Equal(0.1, dest[0].Factor)
	assert.Equal(deftime, dest[0].CreatedAt)

	assert.Equal(3, dest[1].ID)
	assert.Equal(`Second`, dest[1].Name)
	assert.Equal(0.2, dest[1].Factor)
	assert.Equal(othtime, dest[1].CreatedAt)

	assert.Equal(5, dest[2].ID)
	assert.Equal(`Third`, dest[2].Name)
	assert.Equal(0.3, dest[2].Factor)
	assert.Equal(deftime, dest[2].CreatedAt)
}
