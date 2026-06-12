package main

import (
	"testing"

	"github.com/goccy/bigquery-emulator/server"
)

func TestStorageFromOptionsDuckDBMaxMemory(t *testing.T) {
	tests := []struct {
		name string
		opt  option
		want server.Storage
	}{
		{
			name: "temp storage",
			opt:  option{DuckDBMaxMem: "3GB"},
			want: server.Storage("tmp?max_memory=3GB"),
		},
		{
			name: "file storage",
			opt:  option{Database: "/data/finance-emulator.db", DuckDBMaxMem: "3GB"},
			want: server.Storage("file:/data/finance-emulator.db?cache=shared&max_memory=3GB"),
		},
		{
			name: "file storage default",
			opt:  option{Database: "/data/finance-emulator.db"},
			want: server.Storage("file:/data/finance-emulator.db?cache=shared"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := storageFromOptions(tt.opt); got != tt.want {
				t.Fatalf("storageFromOptions = %q; want %q", got, tt.want)
			}
		})
	}
}
