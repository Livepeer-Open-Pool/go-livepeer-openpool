package starter

import (
	"time"

	"github.com/golang/glog"
	lpmon "github.com/livepeer/go-livepeer/monitor"
)

func startKafkaProducer(cfg LivepeerConfig) error {
	if err := startOrchestratorKafkaProducer(cfg); err != nil {
		return err
	}
	if *cfg.KafkaBootstrapServers == "" || *cfg.KafkaGatewayTopic == "" {
		glog.Warning("not starting Kafka producer as producer config values aren't present")
		return nil
	}

	var gatewayHost = ""
	if cfg.GatewayHost != nil {
		gatewayHost = *cfg.GatewayHost
	}

	return lpmon.InitKafkaProducer(
		*cfg.KafkaBootstrapServers,
		*cfg.KafkaUsername,
		*cfg.KafkaPassword,
		*cfg.KafkaGatewayTopic,
		gatewayHost,
	)
}

// startOrchestratorKafkaProducer starts the producer for the orchestrator's
// own events, which have their own topic and do not need the gateway's.
func startOrchestratorKafkaProducer(cfg LivepeerConfig) error {
	if *cfg.KafkaOrchTopic == "" {
		return nil
	}
	if *cfg.KafkaBootstrapServers == "" {
		glog.Warning("not publishing orchestrator events: -kafkaOrchestratorTopic is set without -kafkaBootstrapServers")
		return nil
	}
	// With -kafkaRequireDelivery an event waits for room in a full queue
	// rather than being dropped at once.
	var enqueueTimeout time.Duration
	if *cfg.KafkaRequireDelivery {
		enqueueTimeout = *cfg.KafkaEnqueueTimeout
	}
	return lpmon.InitOrchestratorKafkaProducer(
		*cfg.KafkaBootstrapServers,
		*cfg.KafkaUsername,
		*cfg.KafkaPassword,
		*cfg.KafkaOrchTopic,
		enqueueTimeout,
	)
}
