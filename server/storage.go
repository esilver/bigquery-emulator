package server

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
	if len(s) > len(TempStorage)+1 && s[:len(TempStorage)+1] == string(TempStorage)+"?" {
		return s[len(TempStorage)+1:], true
	}
	return "", false
}

func appendStorageQuery(storage Storage, query string) Storage {
	if query == "" {
		return storage
	}
	separator := "?"
	if containsQuery(string(storage)) {
		separator = "&"
	}
	return Storage(string(storage) + separator + query)
}

func containsQuery(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '?' {
			return true
		}
	}
	return false
}
