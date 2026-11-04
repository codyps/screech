package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"time"
)

// A successful workload must never silently measure an R1 stream in an R3/R5
// campaign. Consumer replicas are explicitly requested as well.
func addStream(js nats.JetStreamContext, config *nats.StreamConfig) (*nats.StreamInfo, error) {
	info, err := js.AddStream(config)
	if err != nil {
		return nil, err
	}
	if info.Config.Replicas != config.Replicas {
		_ = js.DeleteStream(config.Name)
		return nil, fmt.Errorf("stream replicas: requested %d, got %d", config.Replicas, info.Config.Replicas)
	}
	return info, nil
}

func checkConsumer(info *nats.ConsumerInfo, replicas int) error {
	if info.Config.Replicas != replicas {
		return fmt.Errorf("consumer replicas: requested %d, got %d", replicas, info.Config.Replicas)
	}
	return nil
}

// Clustered NATS commits consumer ACKs and stream retention in separate groups.
// Preserve the exact final-state checks, allowing bounded convergence outside
// the timed ACK workload. Standalone validation remains immediate.
func checkConsumed(js nats.JetStreamContext, stream string, expected, stored uint64, replicas int) error {
	wait := time.Duration(0)
	if replicas > 1 {
		wait = 5 * time.Second
	}
	return consumedBy(js, stream, expected, stored, time.Now().Add(wait))
}

func consumedBy(js nats.JetStreamContext, stream string, expected, stored uint64, deadline time.Time) error {
	for {
		consumer, err := js.ConsumerInfo(stream, "D")
		if err != nil {
			return err
		}
		info, err := js.StreamInfo(stream)
		if err != nil {
			return err
		}
		if consumer.NumAckPending == 0 && consumer.AckFloor.Consumer == expected && info.State.Msgs == stored && info.State.LastSeq == expected {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("ACK/retention did not converge: floor=%d pending=%d stored=%d last=%d; expected floor/last=%d stored=%d", consumer.AckFloor.Consumer, consumer.NumAckPending, info.State.Msgs, info.State.LastSeq, expected, stored)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
