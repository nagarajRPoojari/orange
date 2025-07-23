package schema

import (
	"encoding/json"
	"os"
	"path"

	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/types"
)

// 1. take map[string]string,validate types
// 2. take document name and map[string]interface{},
//		load schema for given doc from catalog (so, map[string]string), validate
//		typecast to parrot native types from json types

type SchemaHandlerOpts struct {
	Dir string
}

type SchemaHandler struct {
	opts *SchemaHandlerOpts
}

func NewSchemaHandler(opts *SchemaHandlerOpts) *SchemaHandler {
	return &SchemaHandler{
		opts: opts,
	}
}

func (t *SchemaHandler) VerifySchema(schema query.Schema) error {
	// validating _ID if provided
	_, err := loadSchemaId(schema)
	if err != nil {
		return err
	}

	return recursiveSchemaVerifier(schema)
}

func loadSchemaId(schema query.Schema) (map[string]interface{}, error) {
	var id_map map[string]interface{}
	if _id := schema["_ID"]; _id != nil {
		id_map, ok := _id.(map[string]interface{})
		if !ok {
			return nil, errors.SchemaValidationError("_ID properties missing")
		}
		if ai, ok := id_map["auto_increment"]; ok {
			_, ok := ai.(bool)
			if !ok {
				return nil, errors.SchemaValidationError("auto_increment field should be bool")
			}
		}
	}
	return id_map, nil
}

func recursiveSchemaVerifier(schema map[string]interface{}) error {
	if schema == nil || len(schema) == 0 {
		return errors.SchemaValidationError("missing data type")
	}

	for key, v := range schema {
		if key == "_ID" {
			continue
		}
		// try to cast to string
		vString, ok := v.(string)
		if ok {
			if _, ok := types.AllTypes[vString]; !ok {
				return errors.SchemaValidationError("invalid data type " + vString)
			}
		} else {
			vMap, ok := v.(map[string]interface{})
			if !ok {
				return errors.SchemaValidationError("invalid data type")
			}
			return recursiveSchemaVerifier(vMap)
		}
	}
	return nil
}

func (t *SchemaHandler) SavetoCatalog(docName string, schema query.Schema) error {
	if err := t.VerifySchema(schema); err != nil {
		return err
	}

	bytes, err := json.Marshal(schema)
	if err != nil {
		return errors.SchemaJSONMarshallError("failed to json marshall")
	}

	catalogPath := path.Join(t.opts.Dir, docName)
	if _, err := os.Stat(catalogPath); err != nil {
		if err := os.WriteFile(catalogPath, bytes, 0600); err != nil {
			return errors.SchemaError("failed to save schema to catalog")
		}
	} else {
		return errors.DuplicateSchemaError("for doc: " + docName)
	}
	return nil
}

func (t *SchemaHandler) VerifyAndCastData(schema, data map[string]interface{}) error {
	return recursiveDataCaster(schema, data)
}

func recursiveDataCaster(schema, data map[string]interface{}) error {
	for key, v := range data {
		if key == "_ID" {
			// idMap, err := loadSchemaId(schema)
			// ai, _ := idMap["auto_increment"].(bool)
			// @todo: support auto increment id

			casted, err := types.ToINT64(v)
			if err != nil {
				return err
			}

			data[key] = casted

			continue
		}

		// try to cast to string
		schemaField := schema[key]
		schemaStringField, ok := schemaField.(string)
		if ok {
			casted, err := types.TypeCast(schemaStringField, v)
			if err != nil {
				return err
			}
			data[key] = casted
		} else {
			vMap, ok := v.(map[string]interface{})
			if !ok {
				return errors.TypeCastError("invalid data type of " + key)
			}
			sMap, _ := schemaField.(map[string]interface{})
			return recursiveDataCaster(sMap, vMap)
		}
	}
	return nil
}
