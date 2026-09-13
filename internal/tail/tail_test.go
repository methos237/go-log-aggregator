package tail

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

func batch(t *testing.T, ls model.LabelSet, msgs ...string) (subject string, payload []byte) {
	t.Helper()
	b := &logaggv1.LogBatch{Labels: ls.Proto()}
	for i, m := range msgs {
		rec := model.LogRecord{Time: time.Now(), Seq: int64(i), Level: model.LevelInfo, Message: m}
		b.Records = append(b.Records, rec.Proto())
	}
	payload, err := proto.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Subject("tail", ls.Env, ls.Service), payload
}

func subscribe(t *testing.T, r *Registry, src string) *Subscription {
	t.Helper()
	q, err := query.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Subscribe(q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestSubscriptionReceivesOnlyMatches(t *testing.T) {
	t.Parallel()
	pub := &queuetest.Publisher{}
	r := New(pub, "tail", 8, nil, nil)
	s := subscribe(t, r, `{service="api"} |= "timeout"`)

	api := model.LabelSet{Service: "api", Host: "h", Env: "prod", Extra: map[string]string{"region": "eu"}}
	web := model.LabelSet{Service: "web", Host: "h", Env: "prod"}
	_ = pub.Fanout(batch(t, web, "timeout"))
	_ = pub.Fanout(batch(t, api, "ok", "Timeout!", "another timeout"))

	got := drain(s)
	if len(got) != 2 {
		t.Fatalf("received %d records, want 2: %+v", len(got), got)
	}
	if got[0].Message != "Timeout!" || got[1].Message != "another timeout" {
		t.Errorf("messages = %q, %q; want the two api timeouts in order", got[0].Message, got[1].Message)
	}
	if got[0].Labels.Extra["region"] != "eu" || got[0].StreamID != api.ID() {
		t.Error("record lost its stream labels or id; the client has no other way to learn them")
	}
	if s.Dropped() != 0 {
		t.Errorf("dropped = %d, want 0", s.Dropped())
	}
}

// A pinned selector subscribes narrowly, a regex one to the wildcard; the
// subject filter must never be tighter than the evaluator.
func TestSubscriptionSubjectFollowsTheSelector(t *testing.T) {
	t.Parallel()
	pub := &queuetest.Publisher{}
	r := New(pub, "tail", 8, nil, nil)
	pinned := subscribe(t, r, `{service="api", env="prod"}`)
	loose := subscribe(t, r, `{service=~"a.*"}`)

	staging := model.LabelSet{Service: "api", Host: "h", Env: "staging"}
	_ = pub.Fanout(batch(t, staging, "x"))

	if n := len(drain(pinned)); n != 0 {
		t.Errorf("pinned subscription got %d records from another env, want 0", n)
	}
	if n := len(drain(loose)); n != 1 {
		t.Errorf("regex subscription got %d records, want 1", n)
	}
}

func TestSlowClientDropsWithoutBlocking(t *testing.T) {
	t.Parallel()
	pub := &queuetest.Publisher{}
	r := New(pub, "tail", 2, nil, nil)
	s := subscribe(t, r, `{service="api"}`)

	// Delivery is synchronous in the double, so if a full buffer blocked this
	// call would never return.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = pub.Fanout(batch(t, model.LabelSet{Service: "api", Host: "h", Env: "prod"}, "1", "2", "3", "4", "5"))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fan-out blocked on a full client buffer")
	}
	if got, want := len(drain(s)), 2; got != want {
		t.Errorf("buffered %d records, want %d", got, want)
	}
	if got, want := s.Dropped(), int64(3); got != want {
		t.Errorf("dropped = %d, want %d", got, want)
	}
}

func TestRegistryCloseEndsSubscriptions(t *testing.T) {
	t.Parallel()
	pub := &queuetest.Publisher{}
	r := New(pub, "tail", 8, nil, nil)
	s := subscribe(t, r, `{service="api"}`)

	r.Close()
	select {
	case <-s.Done():
	default:
		t.Fatal("Done not closed after registry Close")
	}
	if _, err := r.Subscribe(&query.Query{Selector: query.Selector{Matchers: []query.Matcher{{Label: "service", Value: "api"}}}}); err != ErrClosed { //nolint:errorlint // sentinel, never wrapped
		t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
	}
	// Idempotent, and a fan-out after the end is simply not delivered.
	s.Close()
	_ = pub.Fanout(batch(t, model.LabelSet{Service: "api", Host: "h", Env: "prod"}, "late"))
	if n := len(drain(s)); n != 0 {
		t.Errorf("received %d records after Close, want 0", n)
	}
}

func TestSubscribeRejectsWhatTheEvaluatorCannotDo(t *testing.T) {
	t.Parallel()
	r := New(&queuetest.Publisher{}, "tail", 8, nil, nil)
	q, err := query.Parse(`{service="api"} | json`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Subscribe(q); err == nil {
		t.Fatal("Subscribe accepted a parser stage")
	}
}

func drain(s *Subscription) []Record {
	var out []Record
	for {
		select {
		case rec := <-s.Records():
			out = append(out, rec)
		default:
			return out
		}
	}
}
