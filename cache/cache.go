package cache

type CacheKiller interface {
	// Delete the cache
	DeleteCache(keys ...string) error

	// Death list. When failed to delete cache, add the key to the death list and wait for the next time to delete it.
	DeathList() []string

	// Remove the key from the death list
	EraseEvidence(keys ...string)

	// Get the indestructibles cache which can not be deleted and need operator to handle.
	Indestructibles() []string
}
