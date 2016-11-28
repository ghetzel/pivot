package generators

import (
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBleveGenerator(t *testing.T) {
	assert := require.New(t)

	tests := map[string]string{
		`id/1`:                                         `+id:1`,
		`str:id/2`:                                     `+id:"2"`,
		`int:id/3`:                                     `+id:3`,
		`float:id/4`:                                   `+id:4`,
		`id/prefix:5`:                                  `+id:"5*"`,
		`id/suffix:6`:                                  `+id:"*6"`,
		`id/contains:7`:                                `+id:"*7*"`,
		`str:id/prefix:8`:                              `+id:"8*"`,
		`str:id/suffix:9`:                              `+id:"*9"`,
		`str:id/contains:10`:                           `+id:"*10*"`,
		`id/not:11`:                                    `-id:11`,
		`str:id/not:12`:                                `-id:"12"`,
		`num/gt:13`:                                    `+num:>13`,
		`num/gte:14`:                                   `+num:>=14`,
		`num/lt:15`:                                    `+num:<15`,
		`num/lte:16`:                                   `+num:<=16`,
		`int:num/gt:17`:                                `+num:>17`,
		`int:num/gte:18`:                               `+num:>=18`,
		`int:num/lt:19`:                                `+num:<19`,
		`int:num/lte:20`:                               `+num:<=20`,
		`float:num/gt:21`:                              `+num:>21`,
		`float:num/gte:22`:                             `+num:>=22`,
		`float:num/lt:23`:                              `+num:<23`,
		`float:num/lte:24`:                             `+num:<=24`,
		`num/gte:0/num/lt:5`:                           `+num:>=0 +num:<5`,
		`id/4/name/contains:Test Phrase`:               `+id:4 +name:"*Test Phrase*"`,
		`name/prefix:Starts With/name/not:Starts With`: `+name:"Starts With*" -name:"Starts With"`,
		`name/prefix:Starts With/name/not:Starts`:      `+name:"Starts With*" -name:Starts`,
		`id/null`:     `+id:""`,
		`id/not:null`: `-id:""`,
	}

	for spec, bleveQSQ := range tests {
		gen := NewBleveGenerator()

		f, err := filter.Parse(spec)
		assert.Nil(err)

		if output, err := filter.Render(gen, `test`, f); err == nil {
			assert.Equal(bleveQSQ, string(output[:]))
		} else {
			assert.Nil(err)
		}
	}
}
