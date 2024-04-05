package database

import "github.com/m4n5ter/cache-killer/cache"

type DBListener interface {
	Listen(killer cache.CacheKiller)
}
