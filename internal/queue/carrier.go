package queue

import "strings"

// HeaderCarrier adapts message headers to the OpenTelemetry propagator.
//
// Not propagation.HeaderCarrier: that one is http.Header underneath and
// canonicalizes keys on Get, so it only finds "Traceparent". Against a real
// broker the key did not round-trip with its case intact: written through the
// SDK carrier, it came back from JetStream as "traceparent" and the
// canonicalizing Get could not see it, so every consumer-side span started a
// new trace. Get here is case-insensitive and Set writes the key as given.
type HeaderCarrier map[string][]string

// Get returns the first value for key, matched case-insensitively.
func (c HeaderCarrier) Get(key string) string {
	if v := c[key]; len(v) > 0 {
		return v[0]
	}
	for k, v := range c {
		if len(v) > 0 && strings.EqualFold(k, key) {
			return v[0]
		}
	}
	return ""
}

// Set stores value as the only value for key.
func (c HeaderCarrier) Set(key, value string) { c[key] = []string{value} }

// Keys lists the header keys.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
