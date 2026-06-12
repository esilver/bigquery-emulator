package server

import "testing"

func TestTempStorageQuery(t *testing.T) {
	query, ok := tempStorageQuery(Storage("tmp?max_memory=3GB"))
	if !ok {
		t.Fatal("tmp storage with query was not recognized as temp storage")
	}
	if query != "max_memory=3GB" {
		t.Fatalf("query = %q; want max_memory=3GB", query)
	}
}

func TestAppendStorageQuery(t *testing.T) {
	got := appendStorageQuery(Storage("file:/tmp/db?cache=shared"), "max_memory=3GB")
	want := Storage("file:/tmp/db?cache=shared&max_memory=3GB")
	if got != want {
		t.Fatalf("appendStorageQuery = %q; want %q", got, want)
	}
}
