package main

import (
	"github.com/nats-io/nats.go"
	"testing"
	"time"
)

type replicationJS struct {
	nats.JetStreamContext
	replicas int
	deleted  bool
}

type consumedJS struct {
	nats.JetStreamContext
	states []struct{ floor, pending, stored, last uint64 }
	index  int
}

func (js *consumedJS) ConsumerInfo(_ string, _ string, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	s := js.states[js.index]
	return &nats.ConsumerInfo{AckFloor: nats.SequenceInfo{Consumer: s.floor}, NumAckPending: int(s.pending)}, nil
}
func (js *consumedJS) StreamInfo(_ string, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	s := js.states[js.index]
	if js.index < len(js.states)-1 {
		js.index++
	}
	return &nats.StreamInfo{State: nats.StreamState{Msgs: s.stored, LastSeq: s.last}}, nil
}

func TestRetentionMayConvergeAfterConfirmedClusterACK(t *testing.T) {
	js := &consumedJS{states: []struct{ floor, pending, stored, last uint64 }{{20, 0, 1, 20}, {20, 0, 0, 20}}}
	if err := consumedBy(js, "T", 20, 0, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct{ floor, pending, stored, last uint64 }{{19, 0, 0, 20}, {20, 1, 0, 20}, {20, 0, 1, 20}, {20, 0, 0, 21}} {
		js := &consumedJS{states: []struct{ floor, pending, stored, last uint64 }{state}}
		if err := consumedBy(js, "T", 20, 0, time.Now()); err == nil {
			t.Fatalf("accepted inconsistent final state: %+v", state)
		}
	}
}

func (js *replicationJS) AddStream(c *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	copy := *c
	copy.Replicas = js.replicas
	return &nats.StreamInfo{Config: copy}, nil
}
func (js *replicationJS) DeleteStream(_ string, _ ...nats.JSOpt) error {
	js.deleted = true
	return nil
}

func TestReplicationMustMatchMeasuredContract(t *testing.T) {
	for _, requested := range []int{1, 3, 5} {
		for _, actual := range []int{1, 3, 5} {
			js := &replicationJS{replicas: actual}
			_, err := addStream(js, &nats.StreamConfig{Name: "T", Replicas: requested})
			if (err != nil) != (requested != actual) || js.deleted != (requested != actual) {
				t.Fatalf("stream requested %d actual %d: err=%v deleted=%v", requested, actual, err, js.deleted)
			}
			err = checkConsumer(&nats.ConsumerInfo{Config: nats.ConsumerConfig{Replicas: actual}}, requested)
			if (err != nil) != (requested != actual) {
				t.Fatalf("consumer requested %d actual %d: %v", requested, actual, err)
			}
		}
	}
}
