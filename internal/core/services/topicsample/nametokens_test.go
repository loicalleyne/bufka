package topicsample

import (
	"maps"
	"slices"
	"testing"
)

func TestTokens(t *testing.T) {
	tests := []struct {
		name string
		got  map[string]bool
		want []string
	}{
		{name: "topic", got: topicTokens("shop.payment-gateway.charge-requests.aol"), want: []string{"aol", "charge", "gateway", "payment", "request", "shop"}},
		{name: "camel fqn", got: messageTokens("Acme.Shop.PaymentGateway.AolChargeRequest"), want: []string{"acme", "aol", "charge", "gateway", "payment", "request", "shop"}},
		{name: "acronym and digits", got: messageTokens("pkg.HTTPServerV2_config"), want: []string{"2", "config", "http", "pkg", "server", "v"}},
		{name: "keeps ss", got: topicTokens("access.class"), want: []string{"access", "class"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slices.Sorted(maps.Keys(tt.got)); !slices.Equal(got, tt.want) {
				t.Errorf("tokens = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAbbreviates(t *testing.T) {
	tests := []struct {
		abbr string
		fqn  string
		want bool
	}{
		{abbr: "aol", fqn: "Acme.Shop.PaymentGateway.AmericaOnLineChargeRequest", want: true},
		{abbr: "ol", fqn: "pkg.OnLine", want: true},
		{abbr: "dsp", fqn: "pkg.DemandSideProvider", want: true},
		{abbr: "ao", fqn: "pkg.Audio", want: false},
		{abbr: "aol", fqn: "Acme.Shop.PaymentGateway.GlobexChargeRequest", want: false},
		{abbr: "o", fqn: "pkg.OnLine", want: false},
		{abbr: "globex", fqn: "pkg.GloBex", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.abbr+" "+tt.fqn, func(t *testing.T) {
			got := slices.ContainsFunc(messageSegments(tt.fqn), func(seg []string) bool { return abbreviates(tt.abbr, seg) })
			if got != tt.want {
				t.Errorf("abbreviates = %v, want %v", got, tt.want)
			}
		})
	}
}
