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

type InternalValueType struct {
	Payload map[string]interface{}
}

// @todo: fix
func (t InternalValueType) SizeOf() uintptr {
	return 8
}

type DBopts struct {
	Dir string
}

type Oragedb struct {
	schemaHandler *schema.SchemaHandler

	dbMap *sync.Map

	opts *DBopts
}

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

func (t *Oragedb) CreateCollection(op query.CreateOp) error {
	db := t.createDB(op.Document)
	t.dbMap.LoadOrStore(op.Document, db)

	return t.schemaHandler.SavetoCatalog(op.Document, op.Schema)
}

func (t *Oragedb) createDB(dbName string) *storage.Storage[types.ID, InternalValueType] {

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

	t.schemaHandler.VerifyAndCastData(schema, res.Value.Payload)

	return res.Value.Payload, nil

}
