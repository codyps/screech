// Official Go client driver. Count/sequence checks are part of each measurement.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	url := flag.String("url", "nats://127.0.0.1:4222", "server URL")
	scenario := flag.String("scenario", "fanout", "fanout, wildcard, queue, core-subject-churn, core-many-subscriptions, core-churn-many-subscriptions, request, durable, consume, consume-one, consume-parallel, consume-ack-window, consume-limits, consume-next, dedup, durable-parallel, consume-one-parallel, mixed-parallel, mixed-streams, core-stream")
	n := flag.Int("n", 100000, "publishes")
	size := flag.Int("size", 128, "payload bytes, at least 8")
	subscribers := flag.Int("subscribers", 1, "subscribers")
	flag.Parse()
	if *n <= 0 || *size < 8 || *subscribers <= 0 {
		return fmt.Errorf("invalid workload")
	}
	var asyncError atomic.Value
	options := []nats.Option{nats.NoReconnect(), nats.Timeout(10 * time.Second), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { asyncError.Store(e.Error()) })}
	pub, e := nats.Connect(*url, options...)
	if e != nil {
		return e
	}
	defer pub.Close()
	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = 'x'
	}
	var elapsed time.Duration
	var deliveries int64
	var latencies []float64
	var durableLatencies []float64
	var fetchLatencies, cycleLatencies []float64
	if *scenario == "consume-one" {
		fetchLatencies = make([]float64, 0, *n)
		cycleLatencies = make([]float64, 0, *n)
	}
	subject := fmt.Sprintf("bench.%d", time.Now().UnixNano())
	if *scenario == "core-stream" {
		elapsed, latencies, durableLatencies, e = runCoreStream(*url, subject, *n, *size, options)
		if e != nil {
			return e
		}
		deliveries = int64(*n)
	} else if concurrentScenario(*scenario) {
		elapsed, deliveries, latencies, e = runConcurrent(*url, *scenario, subject, *n, *size, options)
		if e != nil {
			return e
		}
	} else if *scenario == "request" {
		rc, e := nats.Connect(*url, options...)
		if e != nil {
			return e
		}
		defer rc.Close()
		_, e = rc.Subscribe(subject, func(m *nats.Msg) {
			if e := m.Respond(m.Data); e != nil {
				asyncError.Store(e.Error())
			}
		})
		if e != nil {
			return e
		}
		if e = rc.Flush(); e != nil {
			return e
		}
		start := time.Now()
		for i := 0; i < *n; i++ {
			t := time.Now()
			m, e := pub.Request(subject, payload, 10*time.Second)
			if e != nil {
				return e
			}
			if len(m.Data) != *size {
				return fmt.Errorf("bad reply")
			}
			latencies = append(latencies, float64(time.Since(t).Nanoseconds())/1000)
		}
		elapsed = time.Since(start)
		deliveries = int64(*n)
	} else if *scenario == "consume" || *scenario == "consume-one" || *scenario == "consume-parallel" || *scenario == "consume-ack-window" || *scenario == "consume-limits" || *scenario == "consume-next" {
		js, e := pub.JetStream(nats.MaxWait(15 * time.Second))
		if e != nil {
			return e
		}
		name := fmt.Sprintf("CONSUME_%d", time.Now().UnixNano())
		_, e = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage, Replicas: 1, Retention: func() nats.RetentionPolicy {
			if *scenario == "consume-limits" {
				return nats.LimitsPolicy
			}
			return nats.WorkQueuePolicy
		}()})
		if e != nil {
			return e
		}
		defer js.DeleteStream(name)
		for i := 0; i < *n; i++ {
			binary.LittleEndian.PutUint64(payload, uint64(i))
			if _, e = js.Publish(subject, payload); e != nil {
				return e
			}
		}
		maxPending := 256
		if *scenario == "consume-ack-window" {
			maxPending = 4096
		}
		sub, e := js.PullSubscribe(subject, "D", nats.BindStream(name), nats.AckExplicit(), nats.MaxAckPending(maxPending), nats.AckWait(30*time.Second))
		if e != nil {
			return e
		}
		defer sub.Unsubscribe()
		var ackClients []*nats.Conn
		if *scenario == "consume-parallel" || *scenario == "consume-ack-window" {
			for i := 0; i < 16; i++ {
				c, e := nats.Connect(*url)
				if e != nil {
					return e
				}
				defer c.Close()
				ackClients = append(ackClients, c)
			}
		}
		start := time.Now()
		received := 0
		var nextMessage *nats.Msg
		for received < *n {
			cycleStart := time.Now()
			batch := 128
			if *scenario == "consume-ack-window" {
				batch = 1000
			}
			if *scenario == "consume-one" || *scenario == "consume-next" {
				batch = 1
			}
			if *n-received < batch {
				batch = *n - received
			}
			var messages []*nats.Msg
			if nextMessage != nil {
				messages = []*nats.Msg{nextMessage}
				nextMessage = nil
			} else {
				messages, e = sub.Fetch(batch, nats.MaxWait(5*time.Second))
				if e != nil {
					return e
				}
			}
			if *scenario == "consume-one" {
				fetchLatencies = append(fetchLatencies, float64(time.Since(cycleStart).Nanoseconds())/1000)
			}
			for i, m := range messages {
				meta, e := m.Metadata()
				if e != nil {
					return e
				}
				if len(m.Data) != *size || binary.LittleEndian.Uint64(m.Data) != uint64(received+i) || meta.Sequence.Stream != uint64(received+i+1) || meta.NumPending != uint64(*n-received-i-1) {
					return fmt.Errorf("durable delivery or pending mismatch at %d: %+v", received+i, meta)
				}
			}
			times := make([]float64, len(messages))
			if len(ackClients) > 0 {
				errors := make(chan error, len(ackClients))
				for worker, client := range ackClients {
					go func(worker int, client *nats.Conn) {
						for i := worker; i < len(messages); i += len(ackClients) {
							t := time.Now()
							if _, e := client.Request(messages[i].Reply, []byte("+ACK"), 5*time.Second); e != nil {
								errors <- e
								return
							}
							times[i] = float64(time.Since(t).Nanoseconds()) / 1000
						}
						errors <- nil
					}(worker, client)
				}
				for range ackClients {
					if e := <-errors; e != nil {
						return e
					}
				}
			} else {
				for i, m := range messages {
					t := time.Now()
					if *scenario == "consume-next" {
						if received+i+1 < *n {
							nextMessage, e = pub.Request(m.Reply, []byte("+NXT"), 5*time.Second)
						} else {
							_, e = pub.Request(m.Reply, []byte("+ACK"), 5*time.Second)
						}
					} else {
						e = m.AckSync(nats.AckWait(5 * time.Second))
					}
					if e != nil {
						return e
					}
					times[i] = float64(time.Since(t).Nanoseconds()) / 1000
				}
			}
			if *scenario == "consume-one" {
				cycleLatencies = append(cycleLatencies, float64(time.Since(cycleStart).Nanoseconds())/1000)
			}
			latencies = append(latencies, times...)
			received += len(messages)
		}
		elapsed = time.Since(start)
		deliveries = int64(received)
		ci, e := js.ConsumerInfo(name, "D")
		if e != nil {
			return e
		}
		if ci.NumAckPending != 0 || ci.AckFloor.Consumer != uint64(*n) {
			return fmt.Errorf("durable ACK floor mismatch: %+v", ci)
		}
		si, e := js.StreamInfo(name)
		if e != nil {
			return e
		}
		expectedStored := uint64(0)
		if *scenario == "consume-limits" {
			expectedStored = uint64(*n)
		}
		if si.State.Msgs != expectedStored {
			return fmt.Errorf("retention has %d messages, expected %d", si.State.Msgs, expectedStored)
		}
	} else if *scenario == "dedup" {
		js, e := pub.JetStream(nats.MaxWait(15 * time.Second))
		if e != nil {
			return e
		}
		name := fmt.Sprintf("DEDUP_%d", time.Now().UnixNano())
		_, e = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage, Replicas: 1, Duplicates: 120 * time.Second})
		if e != nil {
			return e
		}
		defer js.DeleteStream(name)
		start := time.Now()
		for i := 0; i < *n; i++ {
			binary.LittleEndian.PutUint64(payload, uint64(i/2))
			t := time.Now()
			ack, e := js.Publish(subject, payload, nats.MsgId(fmt.Sprintf("id-%d", i/2)))
			if e != nil {
				return e
			}
			if ack.Sequence != uint64(i/2+1) || ack.Duplicate != (i%2 == 1) {
				return fmt.Errorf("dedup mismatch at %d: %+v", i, ack)
			}
			latencies = append(latencies, float64(time.Since(t).Nanoseconds())/1000)
		}
		elapsed = time.Since(start)
		deliveries = int64((*n + 1) / 2)
		for i := 0; i < (*n+1)/2; i++ {
			m, e := js.GetMsg(name, uint64(i+1))
			if e != nil {
				return e
			}
			if len(m.Data) != *size || binary.LittleEndian.Uint64(m.Data) != uint64(i) {
				return fmt.Errorf("dedup stored value mismatch")
			}
		}
	} else if *scenario == "durable" {
		js, e := pub.JetStream(nats.MaxWait(15 * time.Second))
		if e != nil {
			return e
		}
		name := fmt.Sprintf("BENCH_%d", time.Now().UnixNano())
		_, e = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage, Replicas: 1})
		if e != nil {
			return e
		}
		defer js.DeleteStream(name)
		start := time.Now()
		for i := 0; i < *n; i++ {
			binary.LittleEndian.PutUint64(payload, uint64(i))
			t := time.Now()
			ack, e := js.Publish(subject, payload)
			if e != nil {
				return e
			}
			if ack.Sequence != uint64(i+1) {
				return fmt.Errorf("ack sequence %d", ack.Sequence)
			}
			latencies = append(latencies, float64(time.Since(t).Nanoseconds())/1000)
		}
		elapsed = time.Since(start)
		deliveries = int64(*n)
		for i := 0; i < *n; i++ {
			m, e := js.GetMsg(name, uint64(i+1))
			if e != nil {
				return e
			}
			if len(m.Data) != *size || binary.LittleEndian.Uint64(m.Data) != uint64(i) {
				return fmt.Errorf("stored message mismatch at %d", i+1)
			}
		}
	} else {
		if !coreBulkScenario(*scenario) {
			return fmt.Errorf("unknown scenario")
		}
		subjectCount, unusedWildcards := coreTopology(*scenario)
		subjects := make([]string, subjectCount)
		for i := range subjects {
			subjects[i] = subject
			if subjectCount > 1 {
				subjects[i] = fmt.Sprintf("%s.s%d", subject, i)
			}
		}
		target := int64(*n)
		if *scenario != "queue" {
			target *= int64(*subscribers)
		}
		done := make(chan struct{})
		var once sync.Once
		var total atomic.Int64
		seen := make([]atomic.Uint32, *n)
		var readers []*nats.Conn
		var readerClosed []chan struct{}
		for s := 0; s < *subscribers; s++ {
			closed := make(chan struct{})
			readerOptions := append([]nats.Option{}, options...)
			readerOptions = append(readerOptions, nats.ClosedHandler(func(_ *nats.Conn) { close(closed) }))
			conn, e := nats.Connect(*url, readerOptions...)
			if e != nil {
				return e
			}
			defer conn.Close()
			readers = append(readers, conn)
			readerClosed = append(readerClosed, closed)
			expected := uint64(0)
			callback := func(m *nats.Msg) {
				if len(m.Data) != *size {
					asyncError.Store("incorrect message size")
					return
				}
				seq := binary.LittleEndian.Uint64(m.Data)
				if seq >= uint64(*n) {
					asyncError.Store("invalid message sequence")
					return
				}
				if m.Subject != subjects[seq%uint64(len(subjects))] {
					asyncError.Store("incorrect delivery subject")
					return
				}
				if *scenario != "queue" {
					if seq != expected {
						asyncError.Store(fmt.Sprintf("gap or duplicate: expected %d got %d", expected, seq))
					}
					expected++
				} else {
					if seen[seq].Add(1) != 1 {
						asyncError.Store("queue duplicate")
					}
				}
				if total.Add(1) == target {
					once.Do(func() { close(done) })
				}
			}
			var sub *nats.Subscription
			if *scenario == "queue" {
				sub, e = conn.QueueSubscribe(subject, "workers", callback)
			} else {
				filter := subject
				if *scenario != "fanout" {
					filter = "bench.>"
				}
				sub, e = conn.Subscribe(filter, callback)
			}
			if e != nil {
				return e
			}
			if e = sub.SetPendingLimits(*n+1, (*n+1)*(*size+128)); e != nil {
				return e
			}
			for i := 0; i < unusedWildcards; i++ {
				if _, e = conn.Subscribe(fmt.Sprintf("unused.%d.*", i), func(_ *nats.Msg) { asyncError.Store("unexpected wildcard delivery") }); e != nil {
					return e
				}
			}
			if e = conn.Flush(); e != nil {
				return e
			}
		}
		flushWindow := func() error {
			if e := pub.Flush(); e != nil {
				return e
			}
			// Publisher PONG follows routing. Each subscriber PONG then follows
			// its queued deliveries, bounding server backlog to one send window.
			for _, reader := range readers {
				if e := reader.Flush(); e != nil {
					return e
				}
			}
			return nil
		}
		start := time.Now()
		for i := 0; i < *n; i++ {
			binary.LittleEndian.PutUint64(payload, uint64(i))
			if e = pub.Publish(subjects[i%len(subjects)], payload); e != nil {
				return e
			}
			if i%1024 == 1023 {
				if e = flushWindow(); e != nil {
					return e
				}
			}
		}
		if e = flushWindow(); e != nil {
			return e
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			return fmt.Errorf("delivery timeout: %d/%d", total.Load(), target)
		}
		elapsed = time.Since(start)
		deliveries = total.Load()
		if *scenario == "queue" {
			for i := range seen {
				if seen[i].Load() != 1 {
					return fmt.Errorf("queue missing %d", i)
				}
			}
		}
		// Drain outside throughput timing so late/unused callbacks are checked too.
		for i, reader := range readers {
			if e := reader.Drain(); e != nil {
				return e
			}
			select {
			case <-readerClosed[i]:
				if e := reader.LastError(); e != nil {
					return e
				}
			case <-time.After(10 * time.Second):
				return fmt.Errorf("Core subscriber drain timeout")
			}
		}
	}
	if e := asyncError.Load(); e != nil {
		return fmt.Errorf("asynchronous error: %v", e)
	}
	result := map[string]any{"scenario": *scenario, "publishes": *n, "payload_bytes": *size, "subscribers": *subscribers, "deliveries": deliveries, "seconds": elapsed.Seconds(), "publishes_per_second": float64(*n) / elapsed.Seconds(), "deliveries_per_second": float64(deliveries) / elapsed.Seconds(), "checked": true}
	switch *scenario {
	case "consume", "consume-one", "consume-parallel", "consume-ack-window", "consume-limits":
		result["latency_scope"] = "confirmed ACK only; Fetch excluded from latency"
	case "consume-next":
		result["latency_scope"] = "ACK + next round trip; final ACK only"
	case "durable", "dedup":
		result["latency_scope"] = "publish-to-durable-ack"
	case "request":
		result["latency_scope"] = "request / reply round trip"
	default:
		result["latency_scope"] = "throughput only; no latency samples"
	}
	if coreBulkScenario(*scenario) {
		result["core_window"] = 1024
		result["core_subjects"], result["unused_wildcards"] = coreTopology(*scenario)
	}
	if *scenario == "consume-ack-window" {
		result["fetch_batch"] = 1000
		result["ack_clients"] = 16
	}
	if concurrentScenario(*scenario) {
		result["streams"] = 1
		result["publishers"] = 16
		result["consumer_connections"] = 0
		result["latency_scope"] = "publish-to-durable-ack"
		if *scenario == "consume-one-parallel" {
			result["publishers"] = 0
			result["consumer_connections"] = 16
			result["latency_scope"] = "fetch-one-through-confirmed-ack"
		} else if *scenario == "mixed-parallel" || *scenario == "mixed-streams" {
			result["publishers"] = 8
			result["consumer_connections"] = 8
			result["latency_scope"] = "fetch-one-through-confirmed-ack"
			if *scenario == "mixed-streams" {
				result["streams"] = 4
			}
		}
	}
	if *scenario == "core-stream" {
		result["latency_scope"] = "publish-to-live-core-callback"
		sort.Float64s(durableLatencies)
		for _, p := range []int{50, 95, 99} {
			result[fmt.Sprintf("durable_p%d_us", p)] = durableLatencies[(len(durableLatencies)-1)*p/100]
		}
	}
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		for _, p := range []int{50, 95, 99} {
			result[fmt.Sprintf("p%d_us", p)] = latencies[(len(latencies)-1)*p/100]
		}
	}
	for prefix, samples := range map[string][]float64{"fetch": fetchLatencies, "cycle": cycleLatencies} {
		if len(samples) == 0 {
			continue
		}
		sort.Float64s(samples)
		for _, p := range []int{50, 95, 99} {
			result[fmt.Sprintf("%s_p%d_us", prefix, p)] = samples[(len(samples)-1)*p/100]
		}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func coreBulkScenario(s string) bool {
	switch s {
	case "fanout", "wildcard", "queue", "core-subject-churn", "core-many-subscriptions", "core-churn-many-subscriptions":
		return true
	}
	return false
}
func coreTopology(s string) (int, int) {
	switch s {
	case "core-subject-churn":
		return 8192, 0
	case "core-many-subscriptions":
		return 1, 512
	case "core-churn-many-subscriptions":
		return 8192, 512
	}
	return 1, 0
}
