package server

import "testing"

func TestCSVDataStartAndHeaderExplicitZero(t *testing.T) {
	records := [][]string{{"1", "alice"}, {"2", "bob"}}
	start, header := csvDataStartAndHeader(records, 0, true, true)
	if start != 0 {
		t.Fatalf("data start = %d; want 0", start)
	}
	if header != nil {
		t.Fatalf("header = %v; want nil", header)
	}
}

func TestCSVDataStartAndHeaderOmittedAutodetect(t *testing.T) {
	records := [][]string{{"id", "name"}, {"1", "alice"}}
	start, header := csvDataStartAndHeader(records, 0, false, true)
	if start != 1 {
		t.Fatalf("data start = %d; want 1", start)
	}
	if len(header) != 2 || header[0] != "id" || header[1] != "name" {
		t.Fatalf("header = %v; want [id name]", header)
	}
}

func TestInferCSVSchemaWithoutHeader(t *testing.T) {
	schema, err := inferCSVSchemaWithoutHeader([][]string{{"1", "alice"}, {"2", "bob"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Fields) != 2 {
		t.Fatalf("field count = %d; want 2", len(schema.Fields))
	}
	if got := schema.Fields[0].Name; got != "string_field_0" {
		t.Fatalf("field[0].Name = %q; want string_field_0", got)
	}
	if got := schema.Fields[0].Type; got != "INTEGER" {
		t.Fatalf("field[0].Type = %q; want INTEGER", got)
	}
}
