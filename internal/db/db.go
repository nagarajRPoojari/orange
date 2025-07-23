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

type _value struct {
	payload map[string]interface{}
}

// @todo: fix
func (t _value) SizeOf() uintptr {
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

func (t *Oragedb) CreateCollection(op query.CreateOp) error {

	dbName := "test"

	const MEMTABLE_THRESHOLD = 1024 * 2
	ctx, _ := context.WithCancel(context.Background())

	db := storage.NewStorage[types.ID, _value](
		dbName,
		ctx,
		storage.StorageOpts{
			Directory:         t.opts.Dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  false,
			TurnOnWal:         false,
		})

	t.dbMap.LoadOrStore(op.Document, db)

	return t.schemaHandler.SavetoCatalog(op.Document, op.Schema)
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
		return errors.InsertError("failed to find document " + op.Document)
	}

	db, ok := val.(*storage.Storage[types.ID, _value])
	if !ok {
		return errors.InsertError("failed to get db for " + op.Document)
	}

	if id, ok := op.Value["_ID"]; ok {
		castedId := types.ID{K: id.(int64)}

		res := db.Put(castedId, _value{payload: op.Value})
		fmt.Println(res)
	}

	return nil
}

func (t *Oragedb) GetDoc(op query.SelectOp) (_value, error) {
	val, ok := t.dbMap.Load(op.Document)
	var null _value
	if !ok {
		return null, errors.InsertError("failed to find document " + op.Document)
	}

	db, ok := val.(*storage.Storage[types.ID, _value])
	if !ok {
		return null, errors.InsertError("failed to get db for " + op.Document)
	}

	castedId := types.ID{K: op.ID}

	res := db.Get(castedId)

	return res.Value, nil

}
