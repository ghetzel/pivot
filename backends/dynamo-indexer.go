package backends

import (
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

func (self *DynamoBackend) IndexConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *DynamoBackend) IndexInitialize(Backend) error {
	return nil
}

func (self *DynamoBackend) GetBackend() Backend {
	return self
}

func (self *DynamoBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.Exists(collection.Name, id)
}

func (self *DynamoBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return self.Retrieve(collection.Name, id)
}

func (self *DynamoBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return nil
}

func (self *DynamoBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return nil
}

func (self *DynamoBackend) QueryFunc(collection *dal.Collection, flt *filter.Filter, resultFn IndexResultFunc) error {
	if err := self.validateFilter(collection, flt); err != nil {
		return fmt.Errorf("Cannot validate filter: %v", err)
	}

	ctx := aws.BackgroundContext()
	pageNumber := 0
	var processed int

	// NOTE: we use the DynamoDB table name instead of .GetIndexName() because we only partially
	// support querying (range scans only), so we're regularly going to be dealing with
	// external indices with potentially-different names.  If this function is being called, that means
	// we're using DynamoDB as an indexer and we want the underlying name intact.

	if flt == nil || flt.IsMatchAll() {
		scan := &dynamodb.ScanInput{
			TableName: aws.String(collection.Name),
			Select:    aws.String(`ALL_ATTRIBUTES`),
		}

		if flt.Limit > 0 {
			scan.SetLimit(int64(flt.Limit))
		}

		return self.db.ScanPagesWithContext(ctx, scan, func(page *dynamodb.ScanOutput, lastPage bool) bool {
			pageNumber += 1

			return self.iterResult(
				collection,
				flt,
				page.Items,
				processed,
				*page.Count,
				pageNumber,
				lastPage,
				resultFn,
			)
		})

	} else {
		query := &dynamodb.QueryInput{
			TableName: aws.String(collection.Name),
			Select:    aws.String(`ALL_ATTRIBUTES`),
		}

		if kcond, _, attrNames, attrValues, attrFieldMap, err := dynamoExprFromFilter(collection, flt); err == nil {
			query = query.SetKeyConditionExpression(strings.Join(kcond, ` AND `))
			query = query.SetExpressionAttributeNames(attrNames)
			query = query.SetExpressionAttributeValues(
				dynamoToDynamoAttributes(collection, attrValues, attrFieldMap),
			)

			return self.db.QueryPagesWithContext(ctx, query, func(page *dynamodb.QueryOutput, lastPage bool) bool {
				pageNumber += 1

				return self.iterResult(
					collection,
					flt,
					page.Items,
					processed,
					*page.Count,
					pageNumber,
					lastPage,
					resultFn,
				)
			})
		} else {
			return fmt.Errorf("Cannot get DynamoDB expression from filter: %v", err)
		}
	}
}

func dynamoExprFromFilter(collection *dal.Collection, flt *filter.Filter) ([]string, []string, map[string]*string, map[string]interface{}, map[string]string, error) {
	fieldId := 0
	valId := 0
	keyCondExpr := []string{}
	condExpr := []string{}
	attrNames := map[string]*string{}
	attrValues := map[string]interface{}{}
	attrFieldMap := map[string]string{}

	if flt != nil {
		for _, criterion := range flt.Criteria {
			if op := dynamoToNativeOp(&criterion); op != `` {
				ors := make([]string, 0)

				for _, value := range criterion.Values {
					ors = append(ors, fmt.Sprintf("#F%d %v :v%d", fieldId, op, valId))
					attrValues[fmt.Sprintf(":v%d", valId)] = value
					attrFieldMap[fmt.Sprintf(":v%d", valId)] = criterion.Field
					valId += 1
				}

				attrNames[fmt.Sprintf("#F%d", fieldId)] = aws.String(criterion.Field)

				// key conditions
				if collection.IsIdentityField(criterion.Field) || collection.IsKeyField(criterion.Field) {
					keyCondExpr = append(keyCondExpr, `(`+strings.Join(ors, ` OR `)+`)`)
				} else {
					condExpr = append(condExpr, `(`+strings.Join(ors, ` OR `)+`)`)
				}

				fieldId += 1
			} else {
				return nil, nil, nil, nil, nil, fmt.Errorf("Unsupported operator '%v'", criterion.Operator)
			}
		}
	}

	return keyCondExpr, condExpr, attrNames, attrValues, attrFieldMap, nil
}

func (self *DynamoBackend) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f != nil {
		f.Options[`ForceIndexRecord`] = true
	}

	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *DynamoBackend) ListValues(collection *dal.Collection, fields []string, flt *filter.Filter) (map[string][]interface{}, error) {
	return nil, fmt.Errorf("%T.ListValues: Not Implemented", self)
}

func (self *DynamoBackend) DeleteQuery(collection *dal.Collection, flt *filter.Filter) error {
	return fmt.Errorf("%T.DeleteQuery: Not Implemented", self)
}

func (self *DynamoBackend) FlushIndex() error {
	return nil
}

func (self *DynamoBackend) validateFilter(collection *dal.Collection, flt *filter.Filter) error {
	if flt != nil {
		for _, field := range flt.CriteriaFields() {
			if collection.IsIdentityField(field) {
				continue
			}

			if collection.IsKeyField(field) {
				continue
			}

			return fmt.Errorf("Filter field '%v' cannot be used: not a key field", field)
		}
	}

	return nil
}

func dynamoToNativeOp(criterion *filter.Criterion) string {
	switch criterion.Operator {
	case `not`:
		return `<>`
	case `lt`:
		return `<`
	case `lte`:
		return `<=`
	case `gt`:
		return `>`
	case `gte`:
		return `>=`
	case `is`, ``:
		return `=`
	default:
		return ``
	}
}

func (self *DynamoBackend) iterResult(collection *dal.Collection, flt *filter.Filter, items []map[string]*dynamodb.AttributeValue, processed int, totalResults int64, pageNumber int, lastPage bool, resultFn IndexResultFunc) bool {
	if len(items) > 0 {
		for _, item := range items {
			record, err := dynamoRecordFromItem(collection, item)

			// fire off the result handler
			if err := resultFn(record, err, IndexPage{
				Page:         pageNumber,
				TotalPages:   int(math.Ceil(float64(totalResults) / float64(25))),
				Limit:        flt.Limit,
				Offset:       (pageNumber - 1) * 25,
				TotalResults: totalResults,
			}); err != nil {
				return false
			}

			// perform bounds checking
			if processed += 1; flt.Limit > 0 && processed >= flt.Limit {
				return false
			}
		}

		return !lastPage
	} else {
		return false
	}
}
