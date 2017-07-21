type IndexStrategy int
const (
    ParallelFirstNonEmpty IndexStrategy = iota
    SequentialFirstNonEmpty
    First
    AllExceptFirst
    Random
)

type MultiIndexer struct {
    RetrievalStrategy IndexStrategy
    PersistStrategy IndexStrategy
    DeleteStrategy IndexStrategy
    indexers []Indexer
    connectionStrings []string
    backend Backend
}

func NewMultiIndexer(connectionStrings ...string) Indexer {
    return &MultiIndexer{
        RetrievalStrategy: ParallelFirstNonEmpty,
        PersistStrategy: AllExceptFirst,
        DeleteStrategy: AllExceptFirst,
        connectionStrings: connectionStrings,
        indexers: make([]Indexer, 0),
    }
}

func (self *MultiIndexer) IndexConnectionString() *dal.ConnectionString {

}

func (self *MultiIndexer) IndexInitialize(backend Backend) error {
    self.backend = backend

    for _, indexer := range self.indexers {
        if err := indexer.IndexInitialize(self.backend); err != nil {
            return err
        }
    }

    return nil
}

func (self *MultiIndexer) IndexExists(collection string, id interface{}) bool {

}

func (self *MultiIndexer) IndexRetrieve(collection string, id interface{}) (*dal.Record, error) {

}

func (self *MultiIndexer) IndexRemove(collection string, ids []interface{}) error {

}

func (self *MultiIndexer) Index(collection string, records *dal.RecordSet) error {

}

func (self *MultiIndexer) QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error {

}

func (self *MultiIndexer) Query(collection string, filter filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {

}

func (self *MultiIndexer) ListValues(collection string, fields []string, filter filter.Filter) (map[string][]interface{}, error) {

}

func (self *MultiIndexer) DeleteQuery(collection string, f filter.Filter) error {

}
