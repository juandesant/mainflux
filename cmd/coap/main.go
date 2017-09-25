package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	coap "github.com/dustin/go-coap"
	"github.com/mainflux/mainflux/coap"
	"github.com/mainflux/mainflux/coap/nats"

	broker "github.com/nats-io/go-nats"
	"go.uber.org/zap"
)

const (
	port       int    = 5683
	defNatsURL string = broker.DefaultURL
	envNatsURL string = "COAP_ADAPTER_NATS_URL"
)

type config struct {
	Port    int
	NatsURL string
}

var logger *zap.Logger = nil

func main() {
	cfg := loadConfig()

	logger, _ = zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any

	nc := connectToNats(cfg)
	defer nc.Close()

	nats.StoreConnection(nc)

	// Initialize map of Observers
	adapter.ObsMap = make(map[string][]adapter.Observer)

	// Subscribe to NATS
	nc.Subscribe("adapter.http", adapter.MsgHandler)
	nc.Subscribe("adapter.mqtt", adapter.MsgHandler)

	logger.Info("Starting CoAP server",
		// Structured context as strongly typed Field values.
		zap.Int("port", cfg.Port),
	)

	errs := make(chan error, 2)

	go func() {
		// Serve CoAP
		coapAddr := fmt.Sprintf(":%d", cfg.Port)
		errs <- coap.ListenAndServe("udp", coapAddr, adapter.COAPServer())
	}()

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()

	c := <-errs
	logger.Info("terminated", zap.String("error", c.Error()))
}

func loadConfig() *config {
	return &config{
		NatsURL: env(envNatsURL, defNatsURL),
		Port:    port,
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func connectToNats(cfg *config) *broker.Conn {
	nc, err := broker.Connect(cfg.NatsURL)
	if err != nil {
		println(logger)
		logger.Error("Failed to connect to NATS", zap.Error(err))
		os.Exit(1)
	}

	return nc
}
