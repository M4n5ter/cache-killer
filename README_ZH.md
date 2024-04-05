# cache-killer

## 概览

cache-killer 是一款高级的缓存删除工具，旨在无缝维护数据库和缓存之间的一致性。cache-killer 的主要目标是通过自动化缓存失效过程来简化缓存管理，改进传统的“更新后删除”模式。它通过解析数据库变更，自动处理缓存失效。

## 功能

- **自动缓存失效**： cache-killer 监听数据库变更，并在不需要显式编写删除缓存操作的情况下，使缓存条目失效。
- **MySQL 支持**：目前 cache-killer 通过读取 MySQL 的 binlog 事件来解析更新和删除操作。
- **缓存键约定**：使用 schema:table_name:primary_key 的约定来构造缓存键模式，然后用其使对应的缓存条目失效。
- **重试机制**：对于任何缓存失效失败的情况，cache-killer 将键放入重试队列，并计划通过定时任务再次尝试删除。
- **失败通知**：达到最大重试次数限制后，cache-killer 会通知系统管理员进行手动干预。
- **计划支持更多数据库和缓存**：未来计划扩展对 PostgreSQL 以及其他缓存数据库的支持。

## 当前限制

- 仅支持 MySQL 和 Redis。
- 计划在未来版本中支持 PostgreSQL 和其他缓存解决方案。

## 快速开始

```bash
    go run . -data <PATH_TO_DATA_FILE> -redis <REDIS_URL>
```
