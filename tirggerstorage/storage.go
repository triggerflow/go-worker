package tirggerstorage

var BackendConstructors = map[string]func(map[string]interface{}) Storage{
	"redis": NewRedisTriggerStorageMappedConfig,
}

// Storage interface defines base tirggerstorage operations
type Storage interface {
	Get(workspace string, key string) map[string]string
	Put(workspace string, key string, field string, value []byte)
}
