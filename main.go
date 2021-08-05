package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	bootstrapServers := flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "topic", "topic to consume from")
	partitions := flag.String("partitions", "0,1,2", "csv list of partitions to consumer from")
	flag.Parse()

	// Partitions csv string to []int32.
	parts := strings.Split(*partitions, ",")
	var partn []int32
	for _, p := range parts {
		pn, err := strconv.Atoi(p)
		exitOnErr(err)

		partn = append(partn, int32(pn))
	}

	// Init the consumer.
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServers,
		"group.id":          "kafkareader",
		"auto.offset.reset": "latest",
	})

	offset, err := kafka.NewOffset("latest")
	exitOnErr(err)

	// Build list of topic/partitions to assign.
	var assignments []kafka.TopicPartition
	for _, p := range partn {
		tp := kafka.TopicPartition{
			Topic:     topic,
			Partition: p,
			Offset:    offset,
		}
		assignments = append(assignments, tp)
	}

	// Assign.
	if err = c.Assign(assignments); err != nil {
		exitOnErr(err)
	}

	var readCount uint64
	var errCount uint64

	// Background a stats thread.
	go func() {
		var lastReadCount uint64
		var currReadCount uint64
		var lastErrCount uint64
		var currErrCount uint64

		t := time.NewTicker(5 * time.Second)
		for range t.C {
			lastReadCount = currReadCount
			currReadCount = atomic.LoadUint64(&readCount)
			lastErrCount = currErrCount
			currErrCount = atomic.LoadUint64(&errCount)

			readSinceLastInterval := currReadCount - lastReadCount
			errSinceLastInterval := currErrCount - lastErrCount
			var errRate float64
			if readSinceLastInterval > 0 {
				errRate = (float64(errSinceLastInterval) / float64(readSinceLastInterval)) * 100
			}

			fmt.Printf("messages read: %d\n", readSinceLastInterval)
			fmt.Printf("error rate: %0.2f%%\n", errRate)
		}
	}()

	// Consume messages and update counters.
	for {
		_, err := c.ReadMessage(-1)
		atomic.AddUint64(&readCount, 1)
		if err != nil {
			atomic.AddUint64(&errCount, 1)
			// Sample errors.
			if atomic.LoadUint64(&errCount)%20==0 {
				fmt.Println(err)
			}
		}
	}

	c.Close()
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
