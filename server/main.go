package main

import (
	"github.com/nats-io/nats-server/v2/server"
	"log"
	"time"
)

func main() {
	natsSrv, err := server.NewServer(&server.Options{
		JetStream: true,
	})
	if err != nil {
		log.Fatalf("Error creating server: %v", err)
	}
	go natsSrv.Start()
	defer natsSrv.Shutdown()

	if !natsSrv.ReadyForConnections(10 * time.Second) {
		log.Fatalf("Unable to connect to NATS Server")
	}
	log.Println("NATS Server is running...")
	select {}
}
