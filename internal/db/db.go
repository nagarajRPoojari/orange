package db

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/nagarajRPoojari/orange/internal/config"
	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/nagarajRPoojari/orange/internal/types"
	storage "github.com/nagarajRPoojari/orange/parrot"
	"github.com/nagarajRPoojari/orange/pkg/query"
	"github.com/nagarajRPoojari/orange/pkg/schema"
)

// InternalValueType wraps map[string]interface{} from query
type InternalValueType struct {
	// since Payload can be any arbitary type, it is advised
	// to only gob registered data types to help unambiguous
	// gob decoding
	Payload map[string]interface{}
	d       bool
}

// @todo: fix
func (t *InternalValueType) SizeOf() uintptr {
	return 8
}

func (t *InternalValueType) MarkDeleted() {
	t.d = true
}

func (t *InternalValueType) IsDeleted() bool {
	return t.d
}

type DBopts struct {
}

// Oragedb represents the core database engine, holding the schema
// handler, a map of database instances, and configuration options
type Oragedb struct {
	schemaHandler *schema.SchemaHandler

	dbMap *sync.Map

	// context for smooth teardown
	context context.Context

	conf config.Config
}

// NewOrangedb initializes the Oragedb instance with schema and config setup
func NewOrangedb(context context.Context, conf config.Config) *Oragedb {

	return &Oragedb{
		schemaHandler: schema.NewSchemaHandler(
			&schema.SchemaHandlerOpts{
				Dir: path.Join(conf.Directory, "catalog"),
			},
		),
		context: context,
		dbMap:   &sync.Map{},
		conf:    conf,
	}
}

// ProcessQuery parses and routes a query to the appropriate database operation
// ProcessQuery is depricated and will be moved out of db.go
// Orangedb doesn't directly accepts query string, should be parsed outside & passed query.Op
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
	case query.DeleteOp:
		return nil, t.DeleteDoc(v)
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}

// CreateCollection creates a new collection and stores its schema in the catalog
func (t *Oragedb) CreateCollection(op query.CreateOp) error {
	db := t.createDB(op.Document)
	t.dbMap.LoadOrStore(op.Document, db)

	return t.schemaHandler.SavetoCatalog(op.Document, op.Schema)
}

// createDB initializes a new parrot instance
func (t *Oragedb) createDB(dbName string) *storage.Storage[types.ID, *InternalValueType] {
	db := storage.NewStorage[types.ID, *InternalValueType](
		dbName,
		t.context,
		storage.StorageOpts{
			Directory:                     path.Join(t.conf.Directory, dbName),
			TurnOnMemtableWal:             t.conf.Memtable.TurnOnWAL,
			MemtableThreshold:             t.conf.Memtable.Threshold,
			MemtableWALTimeInterval:       t.conf.Memtable.WALTimeInterval,
			MemtableWALEventChSize:        t.conf.Memtable.WALEventChSize,
			MemtableWALWriterBufferSize:   t.conf.Memtable.WALWriterBufferSize,
			FlushTimeInterval:             t.conf.Memtable.FlushTimeInterval,
			TurnOnCompaction:              t.conf.Compaction.TurnOn,
			CompactionTimeInterval:        t.conf.Compaction.TimeInterval,
			CompactionWALTimeInterval:     t.conf.Compaction.WALTimeInterval,
			CompactionWALEventChSize:      t.conf.Compaction.WALEventChSize,
			CompactionWALWriterBufferSize: t.conf.Compaction.WALWriterBufferSize,
			Level0MaxSizeInBytes:          t.conf.Compaction.Level0MaxSizeInBytes,
			MaxSizeInBytesGrowthFactor:    t.conf.Compaction.MaxSizeInBytesGrowthFactor,
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

	db, ok := val.(*storage.Storage[types.ID, *InternalValueType])
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

		res := db.Put(castedId, &InternalValueType{Payload: op.Value})
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

	db, ok := val.(*storage.Storage[types.ID, *InternalValueType])
	if !ok {
		return nil, errors.SelectError("failed to get db for " + op.Document)
	}

	castedId := types.ID{K: op.ID}
	res := db.Get(castedId)

	if res.Err != nil {
		return nil, res.Err
	}

	// verifying loaded data & typecasting back to compatible schema types
	t.schemaHandler.VerifyAndCastData(schema, res.Value.Payload)

	return res.Value.Payload, nil
}

// DeleteDoc deletes a document by ID from the specified collection.
func (t *Oragedb) DeleteDoc(op query.DeleteOp) error {
	val, ok := t.dbMap.Load(op.Document)
	if !ok {
		db := t.createDB(op.Document)
		t.dbMap.LoadOrStore(op.Document, db)

		val = db
	}

	db, ok := val.(*storage.Storage[types.ID, *InternalValueType])
	if !ok {
		return errors.DeleteError("failed to delete db for " + op.Document)
	}

	castedId := types.ID{K: op.ID}
	res := db.Delete(castedId, &InternalValueType{})
	return res.Err
}
