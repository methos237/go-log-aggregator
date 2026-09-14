package queue

import "strings"

// HeaderCarrier adapts message headers to the OpenTelemetry propagator.
//
// Not propagation.HeaderCarrier: that one is http.Header underneath and
// canonicalizes keys on both Set and Get, while NATS preserves key case on the
// wire. A traceparent that comes back in a different case than it was written
// is then invisible to a canonicalizing Get, which is exactly what happened
// against a real broker. Get here is case-insensitive and Set writes the key
// as given.
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
