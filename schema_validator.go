package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// SchemaValidator validates JSON responses against an OpenAPI spec snapshot.
type SchemaValidator struct {
	schemas map[string]schemaDefinition
}

type schemaDefinition struct {
	Type       string                      `json:"type"`
	Properties map[string]schemaProperty   `json:"properties"`
	Required   []string                    `json:"required"`
}

type schemaProperty struct {
	Type    interface{} `json:"type"` // string or []string (for nullable)
	Nullable bool       `json:"nullable"`
}

// LoadValidator loads the OpenAPI spec from testdata/openapi.json.
func LoadValidator(path string) (*SchemaValidator, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec: %w", err)
	}

	var spec struct {
		Components struct {
			Schemas map[string]schemaDefinition `json:"schemas"`
		} `json:"components"`
	}

	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing spec: %w", err)
	}

	return &SchemaValidator{schemas: spec.Components.Schemas}, nil
}

// ValidateResponse checks that a JSON response body matches the named schema.
func (v *SchemaValidator) ValidateResponse(t *testing.T, schemaName string, body []byte) {
	t.Helper()

	schema, ok := v.schemas[schemaName]
	if !ok {
		t.Errorf("schema %q not found in OpenAPI spec", schemaName)
		return
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Errorf("response is not valid JSON: %v", err)
		return
	}

	// Check required fields
	for _, req := range schema.Required {
		if _, exists := obj[req]; !exists {
			t.Errorf("schema %s: missing required field %q", schemaName, req)
		}
	}

	// Check field types for fields that exist
	for fieldName, fieldVal := range obj {
		prop, exists := schema.Properties[fieldName]
		if !exists {
			continue // extra fields are OK
		}

		if fieldVal == nil {
			if !prop.Nullable && !isNullableType(prop.Type) {
				t.Errorf("schema %s: field %q is null but not nullable", schemaName, fieldName)
			}
			continue
		}

		expectedTypes := normalizeType(prop.Type)
		actualType := jsonType(fieldVal)
		if !typeMatches(actualType, expectedTypes) {
			t.Errorf("schema %s: field %q expected type %v, got %s (value: %v)",
				schemaName, fieldName, expectedTypes, actualType, fieldVal)
		}
	}
}

func normalizeType(t interface{}) []string {
	switch v := t.(type) {
	case string:
		return []string{v}
	case []interface{}:
		result := make([]string, len(v))
		for i, s := range v {
			result[i] = fmt.Sprintf("%v", s)
		}
		return result
	}
	return []string{"string"}
}

func isNullableType(t interface{}) bool {
	types := normalizeType(t)
	for _, tp := range types {
		if tp == "null" {
			return true
		}
	}
	return false
}

func typeMatches(actual string, expected []string) bool {
	for _, e := range expected {
		if e == actual {
			return true
		}
		// "integer" matches "number" in JSON
		if e == "integer" && actual == "number" {
			return true
		}
	}
	return false
}

func jsonType(v interface{}) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// HasSchema returns true if the named schema exists in the spec.
func (v *SchemaValidator) HasSchema(name string) bool {
	_, ok := v.schemas[name]
	return ok
}

// SchemaNames returns all available schema names.
func (v *SchemaValidator) SchemaNames() []string {
	names := make([]string, 0, len(v.schemas))
	for name := range v.schemas {
		names = append(names, name)
	}
	return names
}

// PrintSchemas prints all schema names (for debugging).
func (v *SchemaValidator) PrintSchemas() {
	fmt.Println("Available schemas:")
	for _, name := range v.SchemaNames() {
		fmt.Printf("  - %s\n", name)
	}
}

// FindSchemaForPath finds likely schema name for a given API path.
func FindSchemaForPath(path string) string {
	path = strings.TrimPrefix(path, "/api/v1/")

	switch {
	case strings.HasPrefix(path, "tasks") && !strings.Contains(path, "executions"):
		return "TaskResponse"
	case strings.Contains(path, "executions"):
		return "ExecutionResponse"
	case strings.HasPrefix(path, "endpoints"):
		return "EndpointResponse"
	default:
		return ""
	}
}
