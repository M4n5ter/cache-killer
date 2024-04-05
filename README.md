# cache-killer

[中文](README_ZH.md)

## Overview

cache-killer is an advanced cache invalidation tool designed to maintain the consistency between your database and cache seamlessly. The primary goal of cache-killer is to simplify the process of cache management by automating the cache invalidation process. It aims to improve the traditional 'update-then-invalidate' model by parsing database changes and handling the cache invalidation automatically.

## Features

- **Automatic Cache Invalidation**: cache-killer listens to database changes and invalidates cache entries without the need to explicitly code the cache delete operations.
- **MySQL Support**: Currently, cache-killer supports MySQL databases by reading binlog events to parse update and delete operations.
- **Cache Key Convention**: It uses a convention schema:table_name:primary_key to form the cache key patterns which are then used to invalidate the corresponding cache entries.
- **Retrying Mechanism**: For any cache invalidation failures, cache-killer places the keys into a retry queue and a scheduled task attempts deletion again.
- **Failure Notification**: Upon reaching the maximum retry limit, cache-killer alerts the system administrator for manual intervention.
- **Planned Support for Additional Databases and Caches**: Future enhancements include extending support to PostgreSQL and other caching databases.

## Current Limitations

- Supports only MySQL and Redis.
- PostgreSQL and other caching solutions are planned for future releases.

## Getting Started

```bash
    go run . -data <PATH_TO_DATA_FILE> -redis <REDIS_URL>
```
