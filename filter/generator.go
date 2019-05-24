package filter

type IGenerator interface {
	Initialize(string) error
	Finalize(*Filter) error
	Push([]byte)
	Set([]byte)
	Payload() []byte
	WithCriterion(Criterion) error
	OrCriterion(Criterion) error
	WithField(string) error
	GroupByField(string) error
	AggregateByField(Aggregation, string) error
	SetOption(string, interface{}) error
	GetValues() []interface{}
	Reset()
}

type Generator struct {
	IGenerator
	payload []byte
}

func Render(generator IGenerator, collectionName string, filter *Filter) ([]byte, error) {
	if err := generator.Initialize(collectionName); err != nil {
		return nil, err
	}

	//  add options
	for key, value := range filter.Options {
		if err := generator.SetOption(key, value); err != nil {
			return nil, err
		}
	}

	//  add fields
	for _, fieldName := range filter.Fields {
		if filter.IdentityField != `` && fieldName == `id` {
			fieldName = filter.IdentityField
		}

		if err := generator.WithField(fieldName); err != nil {
			return nil, err
		}
	}

	//  add criteria
	for _, criterion := range filter.Criteria {
		if filter.IdentityField != `` && criterion.Field == `id` {
			criterion.Field = filter.IdentityField
		}

		if err := generator.WithCriterion(criterion); err != nil {
			return nil, err
		}
	}

	//  finalize the payload
	if err := generator.Finalize(filter); err != nil {
		return nil, err
	}

	//  return the payload
	return generator.Payload(), nil
}

func (self *Generator) Set(data []byte) {
	self.payload = data
}

func (self *Generator) Push(data []byte) {
	if self.payload == nil {
		self.payload = make([]byte, 0)
	}

	self.payload = append(self.payload, data...)
}

func (self *Generator) Reset() {
	self.payload = nil
}

func (self *Generator) Payload() []byte {
	return self.payload
}

func (self *Generator) Finalize(_ *Filter) error {
	return nil
}
