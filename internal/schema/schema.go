package schema

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"

	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/types"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

type SchemaHandlerOpts struct {
	Dir string
}

// SchemaHandler manages document schemas, including caching and validation.
type SchemaHandler struct {
	// cache of loaded schema
	cache map[string]query.Schema

	opts *SchemaHandlerOpts
}

// NewSchemaHandler creates a new SchemaHandler with the given options.
// Initializes an empty in-memory schema cache
func NewSchemaHandler(opts *SchemaHandlerOpts) *SchemaHandler {
	return &SchemaHandler{
		cache: map[string]query.Schema{},
		opts:  opts,
	}
}

// VerifySchema checks the validity of a given schema.
// Validates the presence and type of _ID and all nested fields.
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

// recursiveSchemaVerifier recursively scans schema and validates give type
// and ensures it is supported natively by parrot
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
				return errors.SchemaValidationError("invalid data type %v", vString)
			}
		} else {
			vMap, ok := v.(map[string]interface{})
			if !ok {
				return errors.SchemaValidationError("invalid data type %v", vMap)
			}
			return recursiveSchemaVerifier(vMap)
		}
	}
	return nil
}

// SavetoCatalog saves schema to catalog directory,
// might throw error if duplicate document name found
func (t *SchemaHandler) SavetoCatalog(docName string, schema query.Schema) error {
	if err := t.VerifySchema(schema); err != nil {
		return err
	}

	bytes, err := json.Marshal(schema)
	if err != nil {
		return errors.SchemaJSONMarshallError("%v", err)
	}

	catalogPath := path.Join(t.opts.Dir, docName)
	if err := os.MkdirAll(filepath.Dir(catalogPath), 0755); err != nil {
		return errors.SchemaError("failed to create directories for catalog path")
	}

	if _, err := os.Stat(catalogPath); err != nil {

		if err := os.WriteFile(catalogPath, bytes, 0600); err != nil {
			return errors.SchemaError("failed to save schema to catalog")
		}
	} else {
		return errors.DuplicateSchemaError("%s already exists", docName)
	}

	log.Infof("schema saved to catalog")

	return nil
}

// LoadFromCatalog loads schema from catalog
// @todo: cache loaded schema
func (t *SchemaHandler) LoadFromCatalog(docName string) (query.Schema, error) {
	if schema, ok := t.cache[docName]; ok {
		return schema, nil
	}

	catalogPath := path.Join(t.opts.Dir, docName)
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return nil, errors.SchemaError("failed to load schema")
	}

	var schema query.Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, errors.SchemaJSONUnmarshallError("%v", err)
	}

	return schema, nil
}

// VerifyAndCastData verifies strict schema and tries for
// possible type conversion,
//
// catches:
//   - missing fields
//   - invalid data type
//   - @todo: constraint
func (t *SchemaHandler) VerifyAndCastData(schema, data map[string]interface{}) error {
	err := recursiveDataCaster(schema, data)
	if err != nil {
		return err
	}

	missing := make([]string, 0)
	// for now: user must provide id
	if _, ok := data["_ID"]; !ok {
		missing = append(missing, "_ID")
	}

	for key := range schema {
		if _, ok := data[key]; !ok {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		return errors.MissingFields("%v", missing)
	}

	return nil

}

func recursiveDataCaster(schema, data map[string]interface{}) error {

	for key, v := range data {
		if key == "_ID" {
			// idMap, err := loadSchemaId(schema)
			// ai, _ := idMap["auto_increment"].(bool)
			// @todo: support auto increment id

			casted, err := types.ToID(v)
			if err != nil {
				return err
			}

			data[key] = casted

			continue
		}

		// try to cast to string
		schemaField, ok := schema[key]
		if !ok {
			return errors.UnknownField("%v", key)
		}

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
				return errors.TypeCastError("invalid data type %T", key)
			}
			sMap, _ := schemaField.(map[string]interface{})
			return recursiveDataCaster(sMap, vMap)
		}
	}
	return nil
}
