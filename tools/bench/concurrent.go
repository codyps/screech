package main

import (
	"encoding/binary"
	"fmt"
	"github.com/nats-io/nats.go"
	"sync/atomic"
	"time"
)

func concurrentScenario(s string) bool {
	return s == "durable-parallel" || s == "consume-one-parallel" || s == "mixed-parallel" || s == "mixed-streams"
}

// Each connection uses ordinary synchronous publish, Fetch(1), and AckSync calls.
// No Screech-specific batching or protocol extension is used.
func runConcurrent(url, scenario, subject string, count, size, replicas int, options []nats.Option, profileGate string) (time.Duration, int64, []float64, error) {
	control, err := nats.Connect(url, options...)
	if err != nil {
		return 0, 0, nil, err
	}
	defer control.Close()
	js, err := control.JetStream(nats.MaxWait(15 * time.Second))
	if err != nil {
		return 0, 0, nil, err
	}
	streams := 1
	if scenario == "mixed-streams" {
		streams = 4
	}
	publishing := scenario != "consume-one-parallel"
	consuming := scenario != "durable-parallel"
	publishers, consumers := 16, 0
	if consuming {
		publishers, consumers = 0, 16
	}
	if publishing && consuming {
		publishers, consumers = 8, 8
	}
	names := make([]string, streams)
	subjects := make([]string, streams)
	for i := 0; i < streams; i++ {
		names[i] = fmt.Sprintf("GROUP_%d_%d", time.Now().UnixNano(), i)
		subjects[i] = fmt.Sprintf("%s.s%d", subject, i)
		retention := nats.LimitsPolicy
		if consuming {
			retention = nats.WorkQueuePolicy
		}
		if _, err = addStream(js, &nats.StreamConfig{Name: names[i], Subjects: []string{subjects[i]}, Storage: nats.FileStorage, Replicas: replicas, Retention: retention}); err != nil {
			return 0, 0, nil, err
		}
		defer js.DeleteStream(names[i])
		if consuming {
			info, err := js.AddConsumer(names[i], &nats.ConsumerConfig{Durable: "D", Replicas: replicas, AckPolicy: nats.AckExplicitPolicy, AckWait: 30 * time.Second, MaxAckPending: 256, FilterSubject: subjects[i]})
			if err != nil {
				return 0, 0, nil, err
			}
			if err = checkConsumer(info, replicas); err != nil {
				return 0, 0, nil, err
			}
		}
	}
	publishSeq := make([]uint64, count)
	receivedSeq := make([]uint64, count)
	seen := make([]atomic.Bool, count)
	latencies := make([]float64, count)
	if !publishing {
		for i := 0; i < count; i++ {
			body := make([]byte, size)
			binary.LittleEndian.PutUint64(body, uint64(i))
			ack, e := js.Publish(subjects[0], body)
			if e != nil {
				return 0, 0, nil, e
			}
			publishSeq[i] = ack.Sequence
		}
	}
	connections := make([]*nats.Conn, 0, publishers+consumers)
	clients := make([]nats.JetStreamContext, 0, publishers+consumers)
	subs := make([]*nats.Subscription, consumers)
	for i := 0; i < publishers+consumers; i++ {
		connection, e := nats.Connect(url, options...)
		if e != nil {
			for _, c := range connections {
				c.Close()
			}
			return 0, 0, nil, e
		}
		connections = append(connections, connection)
		defer connection.Close()
		client, e := connection.JetStream(nats.MaxWait(15 * time.Second))
		if e != nil {
			return 0, 0, nil, e
		}
		clients = append(clients, client)
		if i >= publishers {
			worker := i - publishers
			stream := worker % streams
			subs[worker], e = client.PullSubscribe(subjects[stream], "D", nats.Bind(names[stream], "D"))
			if e != nil {
				return 0, 0, nil, e
			}
		}
	}
	// Setup, consumer prefill and connections finish before profilers attach.
	// Ordinary benchmark invocations do not use these diagnostic barriers.
	if profileGate != "" {
		if err := profileBarrier(profileGate, ".ready", ".release"); err != nil {
			return 0, 0, nil, err
		}
	}
	gate := make(chan struct{})
	errors := make(chan error, publishers+consumers)
	for worker := 0; worker < publishers; worker++ {
		go func(worker int) {
			<-gate
			var previous uint64
			for i := worker; i < count; i += publishers {
				body := make([]byte, size)
				binary.LittleEndian.PutUint64(body, uint64(i))
				begin := time.Now()
				ack, e := clients[worker].Publish(subjects[worker%streams], body)
				if e != nil {
					errors <- e
					return
				}
				if ack.Duplicate || ack.Stream != names[worker%streams] || ack.Sequence <= previous {
					errors <- fmt.Errorf("invalid concurrent publish ACK: %+v", ack)
					return
				}
				previous = ack.Sequence
				publishSeq[i] = ack.Sequence
				if !consuming {
					latencies[i] = float64(time.Since(begin).Nanoseconds()) / 1000
				}
			}
			errors <- nil
		}(worker)
	}
	for worker := 0; worker < consumers; worker++ {
		go func(worker int) {
			<-gate
			var previous uint64
			for operation := worker; operation < count; operation += consumers {
				begin := time.Now()
				messages, e := subs[worker].Fetch(1, nats.MaxWait(5*time.Second))
				if e != nil {
					errors <- e
					return
				}
				if len(messages) != 1 {
					errors <- fmt.Errorf("expected Fetch(1)")
					return
				}
				message := messages[0]
				meta, e := message.Metadata()
				if e != nil {
					errors <- e
					return
				}
				if len(message.Data) != size {
					errors <- fmt.Errorf("concurrent payload size")
					return
				}
				index := binary.LittleEndian.Uint64(message.Data)
				if index >= uint64(count) || !seen[index].CompareAndSwap(false, true) || meta.Stream != names[worker%streams] || meta.Sequence.Stream <= previous || meta.NumDelivered != 1 {
					errors <- fmt.Errorf("duplicate/order/metadata mismatch: index=%d metadata=%+v", index, meta)
					return
				}
				if int(index)%streams != worker%streams {
					errors <- fmt.Errorf("cross-stream delivery")
					return
				}
				for _, b := range message.Data[8:] {
					if b != 0 {
						errors <- fmt.Errorf("corrupt concurrent payload")
						return
					}
				}
				previous = meta.Sequence.Stream
				receivedSeq[index] = meta.Sequence.Stream
				if e = message.AckSync(nats.AckWait(5 * time.Second)); e != nil {
					errors <- e
					return
				}
				latencies[index] = float64(time.Since(begin).Nanoseconds()) / 1000
			}
			errors <- nil
		}(worker)
	}
	start := time.Now()
	close(gate)
	var failure error
	for i := 0; i < publishers+consumers; i++ {
		if e := <-errors; e != nil && failure == nil {
			failure = e
			for _, c := range connections {
				c.Close()
			}
		}
	}
	elapsed := time.Since(start)
	if failure != nil {
		return 0, 0, nil, failure
	}
	if profileGate != "" {
		if err := profileBarrier(profileGate, ".complete", ".finish"); err != nil {
			return 0, 0, nil, err
		}
	}
	sequences := make([]map[uint64]bool, streams)
	for i := range sequences {
		sequences[i] = make(map[uint64]bool)
	}
	for i, seq := range publishSeq {
		stream := i % streams
		if seq == 0 || sequences[stream][seq] {
			return 0, 0, nil, fmt.Errorf("missing/duplicate publish sequence")
		}
		sequences[stream][seq] = true
		if consuming && (!seen[i].Load() || receivedSeq[i] != seq) {
			return 0, 0, nil, fmt.Errorf("publish/delivery mismatch at %d", i)
		}
		if !consuming {
			message, e := js.GetMsg(names[stream], seq)
			if e != nil {
				return 0, 0, nil, e
			}
			if len(message.Data) != size || binary.LittleEndian.Uint64(message.Data) != uint64(i) {
				return 0, 0, nil, fmt.Errorf("concurrent stored payload mismatch")
			}
			for _, b := range message.Data[8:] {
				if b != 0 {
					return 0, 0, nil, fmt.Errorf("stored payload corrupted")
				}
			}
		}
	}
	for i, name := range names {
		expected := 0
		if count > i {
			expected = 1 + (count-1-i)/streams
		}
		for seq := uint64(1); seq <= uint64(expected); seq++ {
			if !sequences[i][seq] {
				return 0, 0, nil, fmt.Errorf("sequence gap")
			}
		}
		info, e := js.StreamInfo(name)
		if e != nil {
			return 0, 0, nil, e
		}
		if info.State.LastSeq != uint64(expected) {
			return 0, 0, nil, fmt.Errorf("stream highwater mismatch")
		}
		if consuming {
			if e = checkConsumed(js, name, uint64(expected), 0, replicas); e != nil {
				return 0, 0, nil, e
			}
		} else if info.State.Msgs != uint64(expected) {
			return 0, 0, nil, fmt.Errorf("stored count mismatch")
		}
	}
	return elapsed, int64(count), latencies, nil
}
