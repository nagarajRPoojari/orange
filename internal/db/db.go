package db

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/schema"
	"github.com/nagarajRPoojari/orange/internal/types"
	storage "github.com/nagarajRPoojari/orange/parrot"
)

// InternalValueType wraps map[string]interface{} from query
type InternalValueType struct {
	// since Payload can be any arbitary type, it is advised
	// to only gob registered data types to help unambiguous
	// gob decoding
	Payload map[string]interface{}
}

// @todo: fix
func (t InternalValueType) SizeOf() uintptr {
	return 8
}

type DBopts struct {
	Dir string
}

// Oragedb represents the core database engine, holding the schema
// handler, a map of database instances, and configuration options
type Oragedb struct {
	schemaHandler *schema.SchemaHandler

	dbMap *sync.Map

	opts *DBopts
}

// NewOrangedb initializes the Oragedb instance with schema and config setup
func NewOrangedb(opts DBopts) *Oragedb {

	return &Oragedb{
		schemaHandler: schema.NewSchemaHandler(
			&schema.SchemaHandlerOpts{
				Dir: path.Join(opts.Dir, "catalog"),
			},
		),
		dbMap: &sync.Map{},
		opts:  &opts,
	}
}

// ProcessQuery parses and routes a query to the appropriate database operation
func (t *Oragedb) ProcessQuery(q string) (any, error) {
	parser := query.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case query.CreateOp:
		return nil, t.CreateCollection(v)
	case query.InsertOp:
		return nil, t.InsertDoc(v)
	case query.SelectOp:
		return t.GetDoc(v)
	}

	return nil, errors.SQLSyntaxError("invalid op")
}

// CreateCollection creates a new collection and stores its schema in the catalog
func (t *Oragedb) CreateCollection(op query.CreateOp) error {
	db := t.createDB(op.Document)
	t.dbMap.LoadOrStore(op.Document, db)

	return t.schemaHandler.SavetoCatalog(op.Document, op.Schema)
}

// createDB initializes a new parrot instance
func (t *Oragedb) createDB(dbName string) *storage.Storage[types.ID, InternalValueType] {

	// @todo: read from config
	const MEMTABLE_THRESHOLD = 1024 * 2
	ctx, _ := context.WithCancel(context.Background())

	db := storage.NewStorage[types.ID, InternalValueType](
		dbName,
		ctx,
		storage.StorageOpts{
			Directory:         t.opts.Dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			WalLogDir:         path.Join(t.opts.Dir, dbName),
			GCLogDir:          path.Join(t.opts.Dir, dbName),
			TurnOnCompaction:  false,
			TurnOnWal:         true,
		})

	return db

}

// InsertDoc validates and inserts a document into the target collection.
func (t *Oragedb) InsertDoc(op query.InsertOp) error {
	schema, err := t.schemaHandler.LoadFromCatalog(op.Document)
	if err != nil {
		return err
	}

	if err := t.schemaHandler.VerifyAndCastData(schema, op.Value); err != nil {
		return err
	}

	val, ok := t.dbMap.Load(op.Document)
	if !ok {
		db := t.createDB(op.Document)
		t.dbMap.LoadOrStore(op.Document, db)

		val = db
	}

	db, ok := val.(*storage.Storage[types.ID, InternalValueType])
	if !ok {
		return errors.InsertError("failed to get db for " + op.Document)
	}

	if id, ok := op.Value["_ID"]; ok {

		// @todo: need to verify this block
		// id is assumed to be casted to int64 by schemaHandler
		// still id.(int64) fails sometimes

		var castedId types.ID

		switch v := id.(type) {
		case int64:
			castedId = types.ID{K: v}
		case int:
			castedId = types.ID{K: int64(v)}
		case float64:
			castedId = types.ID{K: int64(v)}
		default:
			return fmt.Errorf("unexpected type for id: %T %v", id, id)
		}

		op.Value["_ID"] = castedId

		res := db.Put(castedId, InternalValueType{Payload: op.Value})
		return res.Err
	}

	return nil
}

// GetDoc retrieves a document by ID from the specified collection.
func (t *Oragedb) GetDoc(op query.SelectOp) (map[string]interface{}, error) {
	schema, err := t.schemaHandler.LoadFromCatalog(op.Document)
	if err != nil {
		return nil, err
	}

	val, ok := t.dbMap.Load(op.Document)
	if !ok {
		db := t.createDB(op.Document)
		t.dbMap.LoadOrStore(op.Document, db)

		val = db
	}

	db, ok := val.(*storage.Storage[types.ID, InternalValueType])
	if !ok {
		return nil, errors.SelectError("failed to get db for " + op.Document)
	}

	castedId := types.ID{K: op.ID}
	res := db.Get(castedId)

	// verifying loaded data & typecasting back to compatible schema types
	t.schemaHandler.VerifyAndCastData(schema, res.Value.Payload)

	return res.Value.Payload, nil
}
