package main

import (
	"encoding/binary"
	"fmt"
	"github.com/nats-io/nats.go"
	"sync/atomic"
	"time"
)

// Measure live Core callback latency and durable publish ACK latency separately.
func runCoreStream(url, subject string, count, size, replicas int, options []nats.Option) (time.Duration, []float64, []float64, error) {
	publisher, err := nats.Connect(url, options...)
	if err != nil {
		return 0, nil, nil, err
	}
	defer publisher.Close()
	js, err := publisher.JetStream(nats.MaxWait(15 * time.Second))
	if err != nil {
		return 0, nil, nil, err
	}
	name := fmt.Sprintf("LIVE_%d", time.Now().UnixNano())
	if _, err = addStream(js, &nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage, Replicas: replicas}); err != nil {
		return 0, nil, nil, err
	}
	defer js.DeleteStream(name)
	subscriber, err := nats.Connect(url, options...)
	if err != nil {
		return 0, nil, nil, err
	}
	defer subscriber.Close()
	starts := make([]atomic.Int64, count)
	seen := make([]atomic.Bool, count)
	core := make([]float64, count)
	ackLat := make([]float64, count)
	var received atomic.Int64
	var invalid atomic.Bool
	_, err = subscriber.Subscribe(subject, func(message *nats.Msg) {
		if len(message.Data) != size {
			invalid.Store(true)
			return
		}
		index := binary.LittleEndian.Uint64(message.Data)
		if index >= uint64(count) || index != uint64(received.Load()) || !seen[index].CompareAndSwap(false, true) {
			invalid.Store(true)
			return
		}
		for _, b := range message.Data[8:] {
			if b != 0 {
				invalid.Store(true)
			}
		}
		core[index] = float64(time.Now().UnixNano()-starts[index].Load()) / 1000
		received.Add(1)
	})
	if err != nil {
		return 0, nil, nil, err
	}
	if err = subscriber.Flush(); err != nil {
		return 0, nil, nil, err
	}
	start := time.Now()
	for i := 0; i < count; i++ {
		payload := make([]byte, size)
		binary.LittleEndian.PutUint64(payload, uint64(i))
		begin := time.Now()
		starts[i].Store(begin.UnixNano())
		ack, e := js.Publish(subject, payload)
		if e != nil {
			return 0, nil, nil, e
		}
		if ack.Stream != name || ack.Sequence != uint64(i+1) || ack.Duplicate {
			return 0, nil, nil, fmt.Errorf("invalid live-stream durable ACK")
		}
		ackLat[i] = float64(time.Since(begin).Nanoseconds()) / 1000
	}
	deadline := time.Now().Add(5 * time.Second)
	for received.Load() != int64(count) {
		if invalid.Load() || time.Now().After(deadline) {
			return 0, nil, nil, fmt.Errorf("live Core count/order/payload mismatch: %d", received.Load())
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)
	if invalid.Load() {
		return 0, nil, nil, fmt.Errorf("live Core mismatch")
	}
	for i := 0; i < count; i++ {
		message, e := getStoredMessage(publisher, js, name, uint64(i+1), size)
		if e != nil {
			return 0, nil, nil, e
		}
		if len(message.Data) != size || binary.LittleEndian.Uint64(message.Data) != uint64(i) {
			return 0, nil, nil, fmt.Errorf("persisted live-stream payload mismatch")
		}
	}
	if invalid.Load() {
		return 0, nil, nil, fmt.Errorf("late duplicate Core delivery")
	}
	return elapsed, core, ackLat, nil
}
