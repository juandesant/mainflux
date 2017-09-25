// Package nats contains NATS-specific message repository implementation.
package nats

import (
	"encoding/json"

	"github.com/mainflux/mainflux/normalizer"
	broker "github.com/nats-io/go-nats"
)

const topic string = "adapter.coap"

// Stored NATS connection
var snc *broker.Conn

func StoreConnection(nc *broker.Conn) {
	snc = nc
}

func Send(msg normalizer.Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return snc.Publish(topic, b)
}
