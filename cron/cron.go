package cron

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/m4n5ter/cache-killer/cache"
)

type CronJob interface {
	// ExterminateExiles is a cron job that will delete all the exiled cache.
	ExterminateExiles()
}

type cron struct {
	killer               cache.CacheKiller
	indestructibles      map[string]uint8
	indestructibles_chan chan string
}

func NewCron(killer cache.CacheKiller) CronJob {
	return &cron{
		killer:               killer,
		indestructibles:      make(map[string]uint8),
		indestructibles_chan: make(chan string, 1),
	}
}

func (c *cron) ExterminateExiles() {
	err := c.schedule()
	if err != nil {
		slog.Error("Failed to schedule the cron job", "error", err)
		os.Exit(1)
	}
}

var _ CronJob = (*cron)(nil)

func (c *cron) schedule() (err error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("Create a new scheduler: %w", err)
	}
	defer s.Shutdown()

	// Delete the cache, try again in 1 second if failed.
	// If failed 5 times, mark it as indestructible.
	_, err = s.NewJob(gocron.DurationJob(1*time.Second), gocron.NewTask(func() {
		if len(c.killer.DeathList()) > 0 {
			err := c.killer.DeleteCache(c.killer.DeathList()...)
			slog.Info("Deleting cache from deadth list", "keys", c.killer.DeathList())
			// Failed to delete cache
			if err != nil {
				for _, key := range c.killer.DeathList() {
					if c.indestructibles[key] > 5 {
						c.killer.EraseEvidence(key)
						c.indestructibles_chan <- key
						continue
					}
					c.indestructibles[key]++
				}
				slog.Error("Failed to delete cache", "error", err)
			}

			// Successfully deleted the cache
			// Remove the key from the death list
			c.killer.EraseEvidence(c.killer.DeathList()...)
		}
	}))
	if err != nil {
		return fmt.Errorf("Create a new job: %w", err)
	}

	// TODO: Alert the operator to handle the indestructibles cache.
	// For now, just log the indestructibles cache.
	go func() {
		for {
			select {
			case key := <-c.indestructibles_chan:
				slog.Error("Indestructible cache", "key", key)
			}
		}
	}()

	s.Start()
	select {}
}
