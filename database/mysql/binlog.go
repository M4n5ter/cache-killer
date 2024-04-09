package mysql

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/m4n5ter/cache-killer/cache"
	"github.com/m4n5ter/cache-killer/database"
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
		slog.Error("Failed to create a new watcher", "error", err)
		os.Exit(1)
	}
	defer watcher.Close()

	m.setup()

	// Open the binlog file
	file, err := os.Open(m.binlogFile)
	if err != nil {
		slog.Error("Failed to open the binlog file", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	binlog_index := path.Join(m.DataDir, "binlog.index")

	// When a binlog update or write event happens, we should delete cache.
	go func(killer cache.CacheKiller, watcher *fsnotify.Watcher, file *os.File, binlog_index string) {
		err = m.run(killer, watcher, file, binlog_index)
		if err != nil {
			slog.Error("Failed to run the binlog listener", "error", err)
			os.Exit(1)
		}
	}(killer, watcher, file, binlog_index)

	watcher.Add(binlog_index)
	watcher.Add(m.binlogFile)
	slog.Info("Listening to the binlog file", "path", m.binlogFile)
	select {}
}

var _ database.DBListener = (*MysqlBinlog)(nil)

func (m *MysqlBinlog) setup() {
	// TODO: Get the data directory from the mysql config file

	if err := m.computeBinlogFilePath(); err != nil {
		slog.Error("Failed to get the newest binlog file path", "error", err)
		os.Exit(1)
	}

	if err := m.mapTable(); err != nil {
		slog.Error("Failed to map table", "error", err)
		os.Exit(1)
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

func (m *MysqlBinlog) run(killer cache.CacheKiller, watcher *fsnotify.Watcher, file *os.File, binlog_index string) (err error) {
	// Cache keys which need to be deleted
	cache_keys := make([]string, 0, 1<<10) // capacity 1K
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				continue
			}

			// Change to new binlog file path when binlog.index is updated
			if event.Op&fsnotify.Write == fsnotify.Write && event.Name == binlog_index {
				slog.Info("Binlog index file updated, start position from", "offset", m.currentOffset)

				// Compute the newest binlog file path
				if err := m.computeBinlogFilePath(); err != nil {
					return fmt.Errorf("Get the newest binlog file path: %w", err)
				}

				// Clear the mapped table
				clear(m.mappedTable)

				// Update the current offset
				m.currentOffset = 4

				// Open the new binlog file
				file, err = os.Open(m.binlogFile)
				if err != nil {
					return fmt.Errorf("Open the new binlog file: %w", err)
				}
				_, err = file.Seek(m.currentOffset, io.SeekStart)
				if err != nil {
					return fmt.Errorf("Update the current offset: %w", err)
				}
			}

			// Parse the binlog file when binlog file is updated
			if event.Op&fsnotify.Write == fsnotify.Write && (event.Name == m.binlogFile || event.Name == binlog_index) {
				slog.Info("Binlog file updated, start position from", "offset", m.currentOffset)

				// TODO: Maybe we need't to parse the FormatDescriptionEvent every time
				if m.currentOffset < 4 {
					m.currentOffset = 4
				} else if m.currentOffset > 4 {
					//  FORMAT_DESCRIPTION event should be read by default always (despite that fact passed offset may be higher than 4)
					if _, err = file.Seek(4, io.SeekStart); err != nil {
						return fmt.Errorf("Seek to the start position: %w", err)
					}

					// Parse the FORMAT_DESCRIPTION event
					if _, err = m.parser.ParseSingleEvent(file, func(be *replication.BinlogEvent) error { return nil }); err != nil {
						return fmt.Errorf("Parse the FORMAT_DESCRIPTION event: %w", err)
					}
				}

				// Parse the binlog file from the current offset
				_, err = file.Seek(m.currentOffset, io.SeekStart)
				if err != nil {
					return fmt.Errorf("Seek to the start position: %w", err)
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
							return fmt.Errorf("Unknown primary key type: %v", t)
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
					return fmt.Errorf("Parse binlog file: %w", err)
				}

				// Update the current offset
				_, err = file.Seek(m.currentOffset, io.SeekStart)
				if err != nil {
					return fmt.Errorf("Update the current offset: %w", err)
				}

			}

		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}

			return fmt.Errorf("Watcher error: %w", err)
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
}
