package asyncapi

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/pkg/schemafield"
)

func loadTestdata(t *testing.T) *Index {
	t.Helper()
	ix, errs := Load([]string{"testdata"})
	for _, e := range errs {
		if !errors.Is(e, ErrExternalRef) {
			t.Fatalf("unexpected load error: %v", e)
		}
	}
	return ix
}

func fieldPaths(m MessageSpec) []string {
	var p []string
	for _, f := range m.Fields {
		p = append(p, f.Path)
	}
	return p
}

func findField(t *testing.T, tp TopicSpec, path string) schemafield.FieldDef {
	t.Helper()
	for _, m := range tp.Messages {
		for _, f := range m.Fields {
			if f.Path == path {
				return f
			}
		}
	}
	t.Fatalf("field %q not found in %s", path, tp.Topic)
	return schemafield.FieldDef{}
}

func TestTopics(t *testing.T) {
	ix := loadTestdata(t)
	var got []string
	for _, tp := range ix.Topics() {
		got = append(got, tp.Topic)
	}
	want := []string{
		"accounts.users",
		"audit-logs.ingestion",
		"catalog.authors",
		"catalog.genres",
		"logistics.docks",
		"logistics.warehouses",
		"platform.users",
		"shop.store-registrations",
		"shop.store-settings",
		"test.external",
		"test.tree",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("topics:\n got %v\nwant %v", got, want)
	}
	// 7 AsyncAPI files (incl. login-claim-spec.yml, which yields no topics);
	// other.json has no asyncapi key and is skipped.
	files := ix.Files()
	if len(files) != 7 {
		t.Fatalf("files: %v", files)
	}
	for _, f := range files {
		if filepath.Base(f) == "other.json" {
			t.Fatal("non-asyncapi file loaded")
		}
	}
}

func TestTopicSpecs(t *testing.T) {
	ix := loadTestdata(t)
	tests := []struct {
		topic, title, specVer, aaVer, summary, cleanup, descPrefix, key string
		messages, eventTypes                                            []string
	}{
		{
			topic: "catalog.authors", title: "Catalog Event Producer", specVer: "1.0.0", aaVer: "2.6.0",
			summary: "Contains the current state of the authors", cleanup: "compact",
			messages: []string{"AuthorEvent"}, eventTypes: []string{""},
			key: "The Author Id is used as the Kafka message key.",
		},
		{
			topic: "shop.store-settings", title: "API Event Producer", specVer: "1.0.1", aaVer: "3.0.0",
			descPrefix: "Published when a store", messages: []string{"UpdatedStoreSettingsEvent"},
			eventTypes: []string{""}, key: "The store Id is used as the Kafka message key.",
		},
		{
			topic: "accounts.users", specVer: "1.0.0", aaVer: "3.0.0",
			title:   "Message produced for the users",
			summary: "Users present on the example platform.", descPrefix: "Users are published",
			messages: []string{"User"}, eventTypes: []string{""},
			key: "The user Id is used as the Kafka message key.",
		},
		{
			topic: "platform.users", title: "Platform Events", specVer: "1.1.0", aaVer: "2.1.0",
			messages: []string{"userCreated"}, eventTypes: []string{"userCreated"},
		},
		{
			topic: "logistics.warehouses", title: "Platform Events", specVer: "1.1.0", aaVer: "2.1.0",
			messages: []string{"warehouseUpdated"}, eventTypes: []string{"warehouseUpdated"},
		},
		{
			topic: "audit-logs.ingestion", title: "Messages produced by the Example Platform Audit Log",
			specVer: "1.0.0", aaVer: "2.4.0", summary: "Audit logs produced on the example platform.",
			descPrefix: "Audit logs are ingested", messages: []string{"AuditLog"}, eventTypes: []string{""},
			key: "The audit log Id is used the as Kafka message key.",
		},
		{
			topic: "test.tree", title: "Cycle", specVer: "0.1.0", aaVer: "2.6.0",
			messages: []string{"Node", "Other"}, eventTypes: []string{"", ""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.topic, func(t *testing.T) {
			tp, ok := ix.Lookup(tc.topic)
			if !ok {
				t.Fatal("not found")
			}
			if tp.SpecTitle != tc.title || tp.SpecVersion != tc.specVer || tp.AsyncAPIVersion != tc.aaVer {
				t.Errorf("spec meta = %q %q %q", tp.SpecTitle, tp.SpecVersion, tp.AsyncAPIVersion)
			}
			if tp.Summary != tc.summary {
				t.Errorf("summary = %q", tp.Summary)
			}
			if !strings.HasPrefix(tp.Description, tc.descPrefix) {
				t.Errorf("description = %q", tp.Description)
			}
			if tp.CleanupPolicy != tc.cleanup {
				t.Errorf("cleanup = %q", tp.CleanupPolicy)
			}
			var names, evs []string
			for _, m := range tp.Messages {
				names = append(names, m.Name)
				evs = append(evs, m.EventType)
			}
			if !slices.Equal(names, tc.messages) || !slices.Equal(evs, tc.eventTypes) {
				t.Errorf("messages = %v events = %v", names, evs)
			}
			if tp.Messages[0].KeyDescription != tc.key {
				t.Errorf("key = %q", tp.Messages[0].KeyDescription)
			}
			if !strings.HasPrefix(tp.SpecFile, "testdata") {
				t.Errorf("spec file = %q", tp.SpecFile)
			}
		})
	}
}

func TestMergedTopic(t *testing.T) {
	spec := `asyncapi: 2.1.0
info: {title: Merge, version: "1"}
channels:
  platform.things/thingCreated:
    publish:
      summary: created
      message: {name: ThingCreated, payload: {type: object, properties: {Id: {type: string}}}}
  platform.things/thingDeleted:
    publish:
      message: {name: ThingDeleted, payload: {type: object, properties: {Id: {type: string}}}}
`
	p := filepath.Join(t.TempDir(), "merge.yaml")
	if err := os.WriteFile(p, []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}
	ix, errs := Load([]string{p})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	tp, ok := ix.Lookup("platform.things")
	if !ok || len(ix.Topics()) != 1 {
		t.Fatalf("topics = %v", ix.Topics())
	}
	if tp.Summary != "created" || len(tp.Messages) != 2 ||
		tp.Messages[0].EventType != "thingCreated" || tp.Messages[1].EventType != "thingDeleted" ||
		tp.Messages[1].Name != "ThingDeleted" {
		t.Fatalf("merged = %+v", tp)
	}
}

func TestFields(t *testing.T) {
	ix := loadTestdata(t)
	type want struct {
		typ, format, desc, def string
		required, nullable     bool
		repeated               bool
		enum                   []string
	}
	currencies := []string{"AUD", "CAD", "EUR", "GBP", "USD", "NZD"}
	tests := []struct {
		topic, path string
		want        want
	}{
		{"catalog.authors", "Id", want{typ: "integer", desc: "Numeric Id of the author", required: true}},
		{"catalog.authors", "Status", want{
			typ: "string", required: true, def: "Active", enum: []string{"Active", "Deleted"},
			desc: "Status of the entity. Entities become deleted when they are removed from the catalog. Not all events support this.",
		}},
		{"catalog.genres", "ParentId", want{typ: "integer", desc: "Parent id of the genre"}},
		{"shop.store-settings", "StoreId", want{typ: "integer", format: "int64", required: true, desc: "Store Id in the shop"}},
		{"shop.store-settings", "Shipping", want{typ: "Shipping", required: true, desc: "All shipping settings"}},
		{"shop.store-settings", "Shipping.Enabled", want{typ: "boolean", required: true, def: "false", desc: "Whether the store has enabled shipping"}},
		// allOf: inherited properties and required.
		{"shop.store-settings", "Shipping.Courier.DefaultCurrency", want{
			typ: "string", required: true, def: "USD", enum: []string{"USD", "CAD", "EUR", "AUD", "GBP"},
			desc: "An __ISO 4217 currency code__. <br/> Only a subset of all currencies are currently supported on the platform.",
		}},
		{"shop.store-settings", "Shipping.Courier.DeliveryMethod", want{typ: "string", required: true, enum: []string{"standard", "express"}, desc: "The delivery method for the courier API (standard or express)"}},
		{"shop.store-settings", "Shipping.Freight.PrioritizedRoutes", want{
			typ: "array<PrioritizedRoute>", repeated: true,
			desc: "A list of routes with their associated priorities. <br/> Route Id is unique in this list, but priority can be set the same for multiple routes.",
		}},
		{"shop.store-settings", "Shipping.Freight.PrioritizedRoutes[].RouteId", want{typ: "string", required: true, desc: "external route id to set the priority of."}},
		{"shop.store-settings", "Storefront.LogoUrl", want{typ: "string", desc: "URL to an image with the logo for the store"}},
		{"accounts.users", "Email", want{typ: "string", format: "email", required: true}},
		{"accounts.users", "Status", want{typ: "string", def: "Inactive", enum: []string{"Inactive", "Active"}}},
		{"accounts.users", "Organizations", want{typ: "map<string,object>", required: true}},
		{"accounts.users", "Organizations{}.Status", want{typ: "string", required: true, def: "Inactive", enum: []string{"Inactive", "Active"}}},
		{"accounts.users", "Organizations{}.EffectivePermissions", want{typ: "array<string>", repeated: true}},
		{"accounts.users", "ApiUser", want{typ: "object", nullable: true, desc: "Information relating to api users"}},
		{"platform.users", "CreatedDate", want{typ: "string", format: "date-time", required: true}},
		{"platform.users", "Version", want{typ: "integer", required: true, enum: []string{"0"}}},
		{"shop.store-registrations", "Store.Currencies.Primary", want{
			typ: "string", required: true, nullable: true, enum: currencies, desc: "The primary currency of the store.",
		}},
		{"shop.store-registrations", "Store.Currencies.Additional", want{typ: "array<string>", repeated: true, enum: currencies}},
		{"audit-logs.ingestion", "UserId", want{typ: "string", nullable: true, desc: "The identifier for the user that produced this audit log."}},
		{"audit-logs.ingestion", "Origin", want{typ: "array<object>", required: true, repeated: true, desc: "The origin of the request"}},
		{"audit-logs.ingestion", "Origin[].Name", want{typ: "string", required: true, desc: "The name of the request origin."}},
		{"audit-logs.ingestion", "Datetime", want{
			typ: "string", format: "date-time", required: true,
			desc: "The point in time in UTC, ISO-8601 format with timezone, that the event captured in this audit log happened.",
		}},
		// JSON-pointer escapes (~0, ~1) and non-string enum values.
		{"test.tree", "a~b/c", want{typ: "string", enum: []string{"1", "true"}}},
	}
	for _, tc := range tests {
		t.Run(tc.topic+"/"+tc.path, func(t *testing.T) {
			tp, ok := ix.Lookup(tc.topic)
			if !ok {
				t.Fatal("topic not found")
			}
			f := findField(t, tp, tc.path)
			w := tc.want
			if f.Type != w.typ || f.Format != w.format || f.Required != w.required ||
				f.Nullable != w.nullable || f.Repeated != w.repeated || f.Default != w.def ||
				!slices.Equal(f.Enum, w.enum) || f.Source != schemafield.SourceAsyncAPI {
				t.Errorf("got %+v\nwant %+v", f, w)
			}
			if w.desc != "" && f.Description != w.desc {
				t.Errorf("description = %q", f.Description)
			}
		})
	}
}

func TestRequiredAndOrder(t *testing.T) {
	ix := loadTestdata(t)
	tp, _ := ix.Lookup("audit-logs.ingestion")
	m := tp.Messages[0]
	if !slices.Equal(m.Required, []string{"OrganizationId", "Datetime", "Origin", "Scope", "Type"}) {
		t.Errorf("required = %v", m.Required)
	}
	want := []string{"OrganizationId", "Datetime", "UserId", "Origin", "Origin[].Name", "Scope", "Type", "OldData", "NewData"}
	if got := fieldPaths(m); !slices.Equal(got, want) {
		t.Errorf("paths = %v", got)
	}
	rs, _ := ix.Lookup("catalog.authors")
	if rs.Messages[0].ContentType != "application/json" || rs.Messages[0].Title != "Author creation and update" {
		t.Errorf("catalog message = %+v", rs.Messages[0])
	}
}

func TestCycle(t *testing.T) {
	ix := loadTestdata(t)
	tp, ok := ix.Lookup("test.tree")
	if !ok {
		t.Fatal("test.tree missing")
	}
	m := tp.Messages[0]
	if got := fieldPaths(m); !slices.Equal(got, []string{"Name", "Parent", "Children"}) {
		t.Fatalf("paths = %v", got)
	}
	if f := findField(t, tp, "Parent"); f.Type != "Node" {
		t.Errorf("Parent type = %q", f.Type)
	}
	if f := findField(t, tp, "Children"); f.Type != "array<Node>" || !f.Repeated {
		t.Errorf("Children = %+v", f)
	}
	if !findField(t, tp, "Name").Required {
		t.Error("Name should be required")
	}
}

func TestExternalRef(t *testing.T) {
	ix, errs := Load([]string{"testdata/cycle.yml"})
	if len(errs) != 1 || !errors.Is(errs[0], ErrExternalRef) || filepath.Base(errs[0].File) != "cycle.yml" {
		t.Fatalf("errs = %v", errs)
	}
	tp, _ := ix.Lookup("test.external")
	if len(tp.Messages) != 1 || len(tp.Messages[0].Fields) != 0 {
		t.Fatalf("external topic = %+v", tp)
	}
}

func TestDepthCap(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("asyncapi: 3.0.0\ninfo: {title: Deep, version: '1'}\nchannels:\n  deep:\n    messages:\n      m:\n        payload:\n")
	indent := "          "
	for range 15 {
		sb.WriteString(indent + "type: object\n" + indent + "properties:\n" + indent + "  a:\n")
		indent += "    "
	}
	sb.WriteString(indent + "type: string\n")
	p := filepath.Join(t.TempDir(), "deep.yml")
	if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	ix, errs := Load([]string{p})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	tp, _ := ix.Lookup("deep")
	if n := len(tp.Messages[0].Fields); n != maxDepth {
		t.Fatalf("fields = %d, want %d", n, maxDepth)
	}
	if tp.Messages[0].Name != "m" {
		t.Errorf("name = %q", tp.Messages[0].Name)
	}
}

func TestLoadErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.yml")
	if err := os.WriteFile(bad, []byte("asyncapi: 2.0.0\nchannels: [unclosed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ix, errs := Load([]string{dir, filepath.Join(dir, "missing")})
	if len(errs) != 2 || errs[0].File != bad {
		t.Fatalf("errs = %v", errs)
	}
	if len(ix.Topics()) != 0 {
		t.Fatal("expected no topics")
	}
}

func TestEnumType(t *testing.T) {
	for _, tc := range []struct {
		vals []any
		want string
	}{
		{[]any{"Active", "Deleted"}, "string"},
		{[]any{1, 2}, "integer"},
		{[]any{1, 2.5}, "number"},
		{[]any{true, false}, "boolean"},
		{[]any{"a", nil}, "string"},
		{[]any{"a", 1}, ""},
		{nil, ""},
	} {
		if got := enumType(tc.vals); got != tc.want {
			t.Errorf("enumType(%v) = %q, want %q", tc.vals, got, tc.want)
		}
	}
}
