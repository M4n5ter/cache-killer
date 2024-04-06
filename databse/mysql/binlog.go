package mysql

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/m4n5ter/cache-killer/cache"
	database "github.com/m4n5ter/cache-killer/databse"
)

type MysqlBinlog struct {
	mappedTable   map[uint64]string         // Mapping of table name and table id
	parser        *replication.BinlogParser // Binlog parser
	DataDir       string                    // Data directory of mysql
	binlogFile    string                    // Binlog file path
	currentOffset int64                     // Current offset of binlog file
}

func NewMysqlBinlog(data_dir string) *MysqlBinlog {
	return &MysqlBinlog{
		mappedTable: make(map[uint64]string),
		parser:      replication.NewBinlogParser(),
		DataDir:     data_dir,
	}
}

func (m *MysqlBinlog) Listen(killer cache.CacheKiller) {
	// listen to the binlog file
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	m.setup()

	// Open the binlog file
	file, err := os.Open(m.binlogFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	binlog_index := path.Join(m.DataDir, "binlog.index")

	// When a binlog update or write event happens, we should delete cache.
	go func() {
		cache_keys := make([]string, 0, 1000)
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Println("watcher.Events is not ok")
					continue
				}

				// Change to new binlog file path when binlog.index is updated
				if event.Op&fsnotify.Write == fsnotify.Write && event.Name == binlog_index {
					slog.Info("Binlog index file updated, start position from", "offset", m.currentOffset)

					// Compute the newest binlog file path
					if err := m.computeBinlogFilePath(); err != nil {
						slog.Error("Failed to get the newest binlog file path", "error", err)
						os.Exit(1)
					}

					// Clear the mapped table
					clear(m.mappedTable)

					// Update the current offset
					m.currentOffset = 4

					// Open the new binlog file
					file, err = os.Open(m.binlogFile)
					if err != nil {
						slog.Error("Failed to open the new binlog file", "error", err)
						os.Exit(1)
					}
					_, err = file.Seek(m.currentOffset, io.SeekStart)
					if err != nil {
						slog.Error("Failed to update the current offset", "error", err)
						os.Exit(1)
					}
				}

				// Parse the binlog file when binlog file is updated
				if event.Op&fsnotify.Write == fsnotify.Write && (event.Name == m.binlogFile || event.Name == binlog_index) {
					slog.Info("Binlog file updated, start position from", "offset", m.currentOffset)

					if m.currentOffset < 4 {
						m.currentOffset = 4
					} else if m.currentOffset > 4 {
						//  FORMAT_DESCRIPTION event should be read by default always (despite that fact passed offset may be higher than 4)
						if _, err = file.Seek(4, io.SeekStart); err != nil {
							log.Fatalf("seek to %d error %v", m.currentOffset, err)
						}

						// Parse the FORMAT_DESCRIPTION event
						if _, err = m.parser.ParseSingleEvent(file, func(be *replication.BinlogEvent) error { return nil }); err != nil {
							log.Fatalf("parse FormatDescriptionEvent error %v", err)
						}
					}

					// Parse the binlog file from the current offset
					_, err = file.Seek(m.currentOffset, io.SeekStart)
					if err != nil {
						log.Fatal(err)
					}

					err = m.parser.ParseReader(file, func(be *replication.BinlogEvent) error {
						switch be.Header.EventType {
						case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
							replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
							slog.Info("A delete cache event happended", "event", be.Header.EventType)

							// Cache key: schema:table:primary_key
							event := be.Event.(*replication.RowsEvent)
							table_id := event.TableID
							table_schema_name := m.mappedTable[table_id]
							// cache_key : schema:table:primary_key
							primary_key := ""
							switch t := event.Rows[0][0].(type) {
							case int:
								primary_key = strconv.FormatInt(int64(t), 10)
							case int32:
								primary_key = strconv.Itoa(int(t))
							case int64:
								primary_key = strconv.Itoa(int(t))
							case string:
								primary_key = t
							default:
								slog.Error("Unknown primary key type", "type", t)
							}
							cache_key := fmt.Sprintf("%s:%s", table_schema_name, primary_key)
							cache_keys = append(cache_keys, cache_key)
						case replication.TABLE_MAP_EVENT:
							// Update the mapping of table name and table id
							event := be.Event.(*replication.TableMapEvent)
							m.updateMappedTable(event)
						}

						m.currentOffset = int64(be.Header.LogPos)
						return nil
					})
					if err != nil {
						slog.Error("Failed to parse binlog file", "error", err)
						os.Exit(1)
					}

					// Update the current offset
					_, err = file.Seek(m.currentOffset, io.SeekStart)
					if err != nil {
						slog.Error("Failed to update the current offset", "error", err)
						os.Exit(1)
					}

				}

			case err, ok := <-watcher.Errors:
				if !ok {
					slog.Error("watcher.Errors is not ok")
					continue
				}
				slog.Error("Watcher error.", "error", err)
			}

			// Delete cache
			if len(cache_keys) > 0 {
				slog.Info("Deleting cache", "keys", cache_keys)

				// Don't need to handle the error here, the cache killer will handle it.
				_ = killer.DeleteCache(cache_keys...)
				// Clear the cache keys
				cache_keys = cache_keys[:0]
			}
		}
	}()

	watcher.Add(binlog_index)
	watcher.Add(m.binlogFile)
	slog.Info("Listening to the binlog file", "path", m.binlogFile)
	select {}
}

var _ database.DBListener = (*MysqlBinlog)(nil)

func (m *MysqlBinlog) setup() {
	// TODO: Get the data directory from the mysql config file

	if err := m.computeBinlogFilePath(); err != nil {
		log.Fatal(err)
	}

	if err := m.mapTable(); err != nil {
		log.Fatal(err)
	}
}

// Get the mapping of table name and table id and set current offset
func (m *MysqlBinlog) mapTable() error {
	// parse the whole binlog file to get the mapping of table name and table id
	p := replication.NewBinlogParser()
	err := p.ParseFile(m.binlogFile, 0, func(be *replication.BinlogEvent) error {
		if be.Header.EventType == replication.TABLE_MAP_EVENT {
			event := be.Event.(*replication.TableMapEvent)
			m.updateMappedTable(event)
		}
		m.currentOffset = int64(be.Header.LogPos)
		return nil
	})

	return err
}

// Update the mapping of table name and table id, table name is the combination of schema and table name like "schema:table"
func (m *MysqlBinlog) updateMappedTable(event *replication.TableMapEvent) {
	table_id := event.TableID
	schema := string(event.Schema)
	table_name := string(event.Table)
	m.mappedTable[table_id] = schema + ":" + table_name
}

// Compute the newest binlog file from the binlog.index file
func (m *MysqlBinlog) computeBinlogFilePath() error {
	index := path.Join(m.DataDir, "binlog.index")

	f, err := os.Open(index)
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64 = -1
	var lastLine []byte
	buf := make([]byte, 1)

	for {
		// Read from the end of the file
		_, err = f.Seek(offset, io.SeekEnd)
		if err != nil {
			return err
		}

		n, err := f.Read(buf)
		if err != nil {
			return err
		}

		// If we have read a newline character and the last line is not empty, we have found the last line
		if buf[0] == '\n' && n > 0 && len(lastLine) > 0 {
			break
		}

		lastLine = append([]byte{buf[0]}, lastLine...)
		offset--
	}

	newest_binlog := strings.Trim(string(lastLine), "\n")
	m.binlogFile = path.Join(m.DataDir, newest_binlog)

	return nil
}
