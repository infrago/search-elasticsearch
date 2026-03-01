# search-elasticsearch

`search-elasticsearch` 是 `search` 模块的 `elasticsearch` 驱动。

## 安装

```bash
go get github.com/infrago/search@latest
go get github.com/infrago/search-elasticsearch@latest
```

## 接入

```go
import (
    _ "github.com/infrago/search"
    _ "github.com/infrago/search-elasticsearch"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[search]
driver = "elasticsearch"
```

## 公开 API（摘自源码）

- `func (d *elasticDriver) Connect(inst *search.Instance) (search.Connection, error)`
- `func (c *elasticConnection) Open() error  { return nil }`
- `func (c *elasticConnection) Close() error { return nil }`
- `func (c *elasticConnection) Capabilities() search.Capabilities`
- `func (c *elasticConnection) SyncIndex(name string, index search.Index) error`
- `func (c *elasticConnection) Clear(name string) error`
- `func (c *elasticConnection) Upsert(index string, rows []Map) error`
- `func (c *elasticConnection) Delete(index string, ids []string) error`
- `func (c *elasticConnection) Search(index string, query search.Query) (search.Result, error)`
- `func (c *elasticConnection) Count(index string, query search.Query) (int64, error)`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
