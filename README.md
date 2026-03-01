# search-elasticsearch

`search` 的 Elasticsearch 驱动。

驱动名：`elasticsearch`（别名：`es`）

## 使用

```go
import _ "github.com/infrago/search-elasticsearch"
```

```toml
[search]
driver = "elasticsearch"
prefix = "demo_"

[search.setting]
server = "http://127.0.0.1:9200"
username = ""
password = ""
api_key = ""
```

## 配置项

- `server`：Elasticsearch 地址
- `username/password`：Basic Auth（可选）
- `api_key`：API Key（可选，优先于 basic auth）
- `prefix`：索引名前缀（可选）
- `timeout`：HTTP 超时（例如 `5s`）

## 映射说明

1. 统一 `Search DSL` 映射到 ES bool/filter/sort/aggs/highlight。
2. `facets` 映射为 `terms aggregation`。
3. `Upsert/Delete` 使用 `_bulk`。
