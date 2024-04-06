package main

import (
	"flag"

	"github.com/m4n5ter/cache-killer/cache/redis"
	"github.com/m4n5ter/cache-killer/cron"
	"github.com/m4n5ter/cache-killer/database/mysql"
)

var (
	data_dir   = flag.String("data", "/var/lib/mysql", "mysql data directory")
	redis_addr = flag.String("redis", "localhost:6379", "redis address")
)

func main() {
	flag.Parse()
	cacheKiller := redis.NewRedisCache(*redis_addr)
	dbListener := mysql.NewMysqlBinlog(*data_dir)
	go dbListener.Listen(cacheKiller)
	cron.NewCron(cacheKiller).ExterminateExiles()
}
