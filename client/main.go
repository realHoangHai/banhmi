package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"sync"
	"time"
)

var Booked = make(map[string]int)
var locker = sync.RWMutex{}

func main() {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	js, err := conn.JetStream()
	if err != nil {
		log.Fatalf("Error creating jetstream: %v", err)
	}

	go func() {
		if err := takeOrder(js); err != nil {
			log.Fatalf("Error taking order: %v", err)
		}
	}()

	go func() {
		err := processBanhmi(js)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		err := deliverBanhmi(js)
		if err != nil {
			panic(err)
		}
	}()

	select {}
}

func takeOrder(js nats.JetStreamContext) error {
	streamInfo, err := js.AddStream(&nats.StreamConfig{
		Name:        "ORDERS",
		Description: "Take order for banh mi from customers",
		Subjects:    []string{"ORDERS.*"},
	})
	if err != nil {
		return err
	}

	consumerInfo, err := js.AddConsumer(streamInfo.Config.Name, &nats.ConsumerConfig{
		Durable:       "MONITOR",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "ORDERS.BANHMI",
	})
	if err != nil {
		return err
	}

	sub, err := js.PullSubscribe(consumerInfo.Config.FilterSubject, consumerInfo.Name, nats.BindStream(consumerInfo.Stream))
	if err != nil {
		return err
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			panic(err)
		}
	}()

	for {
		msgs, err := sub.Fetch(1)
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			return err
		}
		if len(msgs) == 0 {
			continue
		}
		err = msgs[0].Ack()
		if err != nil {
			return err
		}
		pretty("Order " + string(msgs[0].Data) + " is received")
		natsMsg := &nats.Msg{
			Subject: "PROCESS.BANHMI",
			Data:    msgs[0].Data,
		}
		_, err = js.PublishMsg(natsMsg)
		if err != nil {
			return err
		}
	}
}

func processBanhmi(js nats.JetStreamContext) error {
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "PROCESS",
		Description: "Take order for banh mi from customer",
		Subjects:    []string{"PROCESS.BANHMI"},
	})
	if err != nil {
		return err
	}
	_, err = js.QueueSubscribe("PROCESS.BANHMI", "GROUP", func(msg *nats.Msg) {
		locker.Lock()
		if Booked["A"] == 1 {
			locker.Unlock()
			return
		}
		Booked["A"] = 1
		pretty("Oven A is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["A"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.BANHMI", "GROUP", func(msg *nats.Msg) {
		locker.Lock()
		if Booked["B"] == 1 {
			locker.Unlock()
			return
		}
		Booked["B"] = 1
		pretty("Oven B is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["B"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.BANHMI", "GROUP", func(msg *nats.Msg) {
		locker.Lock()
		if Booked["C"] == 1 {
			locker.Unlock()
			return
		}
		Booked["C"] = 1
		pretty("Oven C is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["C"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}
	return nil
}

func notifyDeliveryOrPanic(js nats.JetStreamContext, msg *nats.Msg) {
	natsMsg := &nats.Msg{
		Subject: "ORDERS.DELIVERY",
		Data:    msg.Data,
	}
	_, err := js.PublishMsg(natsMsg)
	if err != nil {
		panic(err)
	}
}

func deliverBanhmi(js nats.JetStreamContext) error {
	conInfo, err := js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:       "DELIVERY",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "ORDERS.DELIVERY",
	})
	if err != nil {
		return err
	}

	sub, err := js.PullSubscribe(conInfo.Config.FilterSubject, conInfo.Name, nats.BindStream(conInfo.Stream))
	if err != nil {
		return err
	}
	defer func() {
		err := sub.Unsubscribe()
		if err != nil {
			panic(err)
		}
	}()

	for {
		msgs, err := sub.Fetch(1)
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			continue
		}
		err = msgs[0].Ack()
		if err != nil {
			return err
		}
		pretty("Order " + string(msgs[0].Data) + " is prepared and ready to serve")
	}
}

func pretty(str interface{}) {
	t := time.Now()
	fmt.Printf("\n<NATS BANHMI STORE> %q Time:%v\n", str, t.Format("03:04:05 PM 2006-01-02"))
}
