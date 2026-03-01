package search_elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/infrago/infra"
	. "github.com/infrago/base"
	"github.com/infrago/search"
)

type elasticDriver struct{}

type elasticConnection struct {
	server string
	user   string
	pass   string
	key    string
	prefix string
	client *http.Client
}

func init() {
	infra.Register("elasticsearch", &elasticDriver{})
	infra.Register("es", &elasticDriver{})
}

func (d *elasticDriver) Connect(inst *search.Instance) (search.Connection, error) {
	server := pickString(inst.Config.Setting, "server", "host", "url")
	if server == "" {
		server = "http://127.0.0.1:9200"
	}
	timeout := 5 * time.Second
	if inst.Config.Timeout > 0 {
		timeout = inst.Config.Timeout
	}
	if v, ok := inst.Config.Setting["timeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	prefix := inst.Config.Prefix
	if prefix == "" {
		prefix = pickString(inst.Config.Setting, "prefix", "index_prefix")
	}
	return &elasticConnection{
		server: strings.TrimRight(server, "/"),
		user:   pickString(inst.Config.Setting, "username", "user"),
		pass:   pickString(inst.Config.Setting, "password", "pass"),
		key:    pickString(inst.Config.Setting, "api_key", "apikey", "key"),
		prefix: prefix,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func (c *elasticConnection) Open() error  { return nil }
func (c *elasticConnection) Close() error { return nil }
func (c *elasticConnection) Capabilities() search.Capabilities {
	return search.Capabilities{
		SyncIndex: true,
		Clear:     true,
		Upsert:    true,
		Delete:    true,
		Search:    true,
		Count:     true,
		Suggest:   false,
		Sort:      true,
		Facets:    true,
		Highlight: true,
		FilterOps: []string{OpEq, OpNe, OpIn, OpNin, OpGt, OpGte, OpLt, OpLte, OpRange},
	}
}

func (c *elasticConnection) SyncIndex(name string, index search.Index) error {
	idx := c.indexName(name)
	payload := Map{}
	if index.Setting != nil {
		payload = cloneMap(index.Setting)
	}
	if len(index.Attributes) > 0 {
		mappings, ok := payload["mappings"].(Map)
		if !ok || mappings == nil {
			mappings = Map{}
		}
		properties := Map{}
		if pp, ok := mappings["properties"].(Map); ok && pp != nil {
			properties = cloneMap(pp)
		}
		for field, v := range index.Attributes {
			properties[field] = Map{"type": elasticFieldType(v.Type)}
		}
		mappings["properties"] = properties
		payload["mappings"] = mappings
	}
	_, err := c.request(http.MethodPut, "/"+url.PathEscape(idx), payload)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "resource_already_exists_exception") {
		return nil
	}
	return err
}

func (c *elasticConnection) Clear(name string) error {
	body := Map{"query": Map{"match_all": Map{}}}
	_, err := c.request(http.MethodPost, "/"+url.PathEscape(c.indexName(name))+"/_delete_by_query", body)
	return err
}

func (c *elasticConnection) Upsert(index string, rows []Map) error {
	if len(rows) == 0 {
		return nil
	}
	idx := c.indexName(index)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if row == nil {
			continue
		}
		id := fmt.Sprintf("%v", row["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		action := Map{"index": Map{"_index": idx, "_id": id}}
		_ = enc.Encode(action)
		_ = enc.Encode(row)
	}
	return c.bulk(buf.Bytes())
}

func (c *elasticConnection) Delete(index string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	idx := c.indexName(index)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, id := range ids {
		action := Map{"delete": Map{"_index": idx, "_id": id}}
		_ = enc.Encode(action)
	}
	return c.bulk(buf.Bytes())
}

func (c *elasticConnection) Search(index string, query search.Query) (search.Result, error) {
	body := buildSearchBody(query)
	respBytes, err := c.request(http.MethodPost, "/"+url.PathEscape(c.indexName(index))+"/_search", body)
	if err != nil {
		return search.Result{}, err
	}

	var resp struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID        string  `json:"_id"`
				Score     float64 `json:"_score"`
				Source    Map     `json:"_source"`
				Highlight Map     `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
		Aggregations Map `json:"aggregations"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return search.Result{}, err
	}

	hits := make([]search.Hit, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		payload := hit.Source
		if payload == nil {
			payload = Map{}
		}
		applyHighlight(payload, hit.Highlight)
		if len(query.Fields) > 0 {
			payload = pickFields(payload, query.Fields)
		}
		hits = append(hits, search.Hit{ID: hit.ID, Score: hit.Score, Payload: payload})
	}

	facets := map[string][]search.Facet{}
	for _, field := range query.Facets {
		agg, ok := resp.Aggregations[field].(Map)
		if !ok {
			continue
		}
		buckets, ok := agg["buckets"].([]Any)
		if !ok {
			continue
		}
		vals := make([]search.Facet, 0, len(buckets))
		for _, one := range buckets {
			bucket, ok := one.(Map)
			if !ok {
				continue
			}
			vals = append(vals, search.Facet{Field: field, Value: fmt.Sprintf("%v", bucket["key"]), Count: toInt64(bucket["doc_count"])})
		}
		facets[field] = vals
	}

	return search.Result{Total: resp.Hits.Total.Value, Took: resp.Took, Hits: hits, Facets: facets, Raw: resp.Aggregations}, nil
}

func (c *elasticConnection) Count(index string, query search.Query) (int64, error) {
	body := buildSearchBody(query)
	respBytes, err := c.request(http.MethodPost, "/"+url.PathEscape(c.indexName(index))+"/_count", Map{"query": body["query"]})
	if err != nil {
		return 0, err
	}
	var resp struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *elasticConnection) bulk(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.server+"/_bulk", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	c.withAuth(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bts, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch bulk failed: %s", strings.TrimSpace(string(bts)))
	}
	return nil
}

func (c *elasticConnection) request(method, path string, body Any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		bts, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(bts)
	}
	req, err := http.NewRequest(method, c.server+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.withAuth(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bts, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("elasticsearch %s %s failed: %s", method, path, strings.TrimSpace(string(bts)))
	}
	return bts, nil
}

func (c *elasticConnection) withAuth(req *http.Request) {
	if c.key != "" {
		req.Header.Set("Authorization", "ApiKey "+c.key)
		return
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
}

func (c *elasticConnection) indexName(name string) string {
	if c.prefix == "" {
		return name
	}
	return c.prefix + name
}

func buildSearchBody(query search.Query) Map {
	must := make([]Any, 0)
	if strings.TrimSpace(query.Keyword) != "" {
		if query.Prefix {
			must = append(must, Map{"query_string": Map{"query": query.Keyword + "*", "analyze_wildcard": true}})
		} else {
			must = append(must, Map{"multi_match": Map{"query": query.Keyword, "fields": []string{"*"}}})
		}
	}
	filters := make([]Any, 0)
	for _, f := range query.Filters {
		if q := toFilterQuery(f); q != nil {
			filters = append(filters, q)
		}
	}
	boolQuery := Map{}
	if len(must) > 0 {
		boolQuery["must"] = must
	}
	if len(filters) > 0 {
		boolQuery["filter"] = filters
	}
	if len(boolQuery) == 0 {
		boolQuery["must"] = []Any{Map{"match_all": Map{}}}
	}

	body := Map{
		"from":  query.Offset,
		"size":  query.Limit,
		"query": Map{"bool": boolQuery},
	}
	if len(query.Sorts) > 0 {
		sorts := make([]Any, 0, len(query.Sorts))
		for _, s := range query.Sorts {
			order := "asc"
			if s.Desc {
				order = "desc"
			}
			sorts = append(sorts, Map{s.Field: Map{"order": order}})
		}
		body["sort"] = sorts
	}
	if len(query.Facets) > 0 {
		aggs := Map{}
		for _, field := range query.Facets {
			aggs[field] = Map{"terms": Map{"field": field}}
		}
		body["aggs"] = aggs
	}
	if len(query.Highlight) > 0 {
		fields := Map{}
		for _, field := range query.Highlight {
			fields[field] = Map{}
		}
		body["highlight"] = Map{"fields": fields}
	}
	if len(query.Fields) > 0 {
		body["_source"] = query.Fields
	}
	return body
}

func toFilterQuery(f search.Filter) Map {
	op := strings.ToLower(strings.TrimSpace(f.Op))
	if op == "" {
		op = search.FilterEq
	}
	switch op {
	case search.FilterEq, "=":
		return Map{"term": Map{f.Field: f.Value}}
	case search.FilterIn:
		vals := f.Values
		if len(vals) == 0 && f.Value != nil {
			vals = []Any{f.Value}
		}
		return Map{"terms": Map{f.Field: vals}}
	case search.FilterGt, ">", search.FilterGte, ">=", search.FilterLt, "<", search.FilterLte, "<=", search.FilterRange:
		r := Map{}
		switch op {
		case search.FilterGt, ">":
			r["gt"] = f.Value
		case search.FilterGte, ">=":
			r["gte"] = f.Value
		case search.FilterLt, "<":
			r["lt"] = f.Value
		case search.FilterLte, "<=":
			r["lte"] = f.Value
		case search.FilterRange:
			if f.Min != nil {
				r["gte"] = f.Min
			}
			if f.Max != nil {
				r["lte"] = f.Max
			}
		}
		if len(r) > 0 {
			return Map{"range": Map{f.Field: r}}
		}
	}
	return nil
}

func pickString(m Map, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cloneMap(src Map) Map {
	if src == nil {
		return Map{}
	}
	out := Map{}
	for k, v := range src {
		out[k] = v
	}
	return out
}

func pickFields(payload Map, fields []string) Map {
	if payload == nil {
		return Map{}
	}
	if len(fields) == 0 {
		return cloneMap(payload)
	}
	out := Map{}
	for _, field := range fields {
		if v, ok := payload[field]; ok {
			out[field] = v
		}
	}
	return out
}

func elasticFieldType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "bool", "boolean":
		return "boolean"
	case "int", "int8", "int16", "int32":
		return "integer"
	case "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return "long"
	case "float", "float32":
		return "float"
	case "float64", "decimal", "number":
		return "double"
	case "timestamp", "datetime", "date", "time":
		return "date"
	case "map", "json", "jsonb":
		return "object"
	default:
		return "keyword"
	}
}

func applyHighlight(payload Map, hl Map) {
	if payload == nil || hl == nil {
		return
	}
	for field, value := range hl {
		switch vv := value.(type) {
		case []Any:
			if len(vv) > 0 {
				payload[field] = vv[0]
			}
		case []string:
			if len(vv) > 0 {
				payload[field] = vv[0]
			}
		case string:
			payload[field] = vv
		}
	}
}

func toInt64(v Any) int64 {
	switch vv := v.(type) {
	case int:
		return int64(vv)
	case int64:
		return vv
	case float64:
		return int64(vv)
	default:
		return 0
	}
}
