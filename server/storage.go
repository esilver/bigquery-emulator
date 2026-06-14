package server

import "strings"

type Storage string

const (
	MemoryStorage Storage = "file::memory:?cache=shared"
	TempStorage   Storage = "tmp"
)

func tempStorageQuery(storage Storage) (string, bool) {
	s := string(storage)
	if s == string(TempStorage) {
		return "", true
	}
	// Match "tmp?" only when a non-empty query follows the separator, so a bare
	// "tmp?" reports no temp query, matching the original prefix-slice behavior.
	if rest, ok := strings.CutPrefix(s, string(TempStorage)+"?"); ok && rest != "" {
		return rest, true
	}
	return "", false
}

func appendStorageQuery(storage Storage, query string) Storage {
	if query == "" {
		return storage
	}
	separator := "?"
	if strings.Contains(string(storage), "?") {
		separator = "&"
	}
	return Storage(string(storage) + separator + query)
}
