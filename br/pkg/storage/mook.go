package storage

type MockStorage struct {
	LocalStorage
}

// NewMockStorage create a `MockStorage` used by ut
func NewMockStorage(path string) *MockStorage {
	return &MockStorage{
		LocalStorage{
			base: path,
		},
	}
}
