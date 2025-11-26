package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FeedPost struct {
	Type        string  `json:"type"`
	UserID      string  `json:"userId"`
	Text        string  `json:"text"`
	DistanceKm  float64 `json:"distanceKm"`
	AvgSpeedKmh float64 `json:"avgSpeedKmh"`
	FinishedAt  string  `json:"finishedAt"`
}

var (
	feed []FeedPost
	mu   sync.Mutex
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func feedHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	if feed == nil {
		feed = []FeedPost{}
	}
	json.NewEncoder(w).Encode(feed)
}

func startConsumer(rabbitURL string) {
	go func() {
		for {
			conn, err := amqp.Dial(rabbitURL)
			if err != nil {
				log.Println("social: cannot connect to rabbit, retry in 2s:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Println("social: cannot open channel, retry in 2s:", err)
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			err = ch.ExchangeDeclare(
				"events_exchange",
				"topic",
				true,
				false,
				false,
				false,
				nil,
			)
			failOnError(err, "social: Failed to declare 'events_exchange'")

			err = ch.ExchangeDeclare(
				"events_dlx",
				"direct",
				true,
				false,
				false,
				false,
				nil,
			)
			failOnError(err, "social: Failed to declare 'events_dlx'")

			dlq, err := ch.QueueDeclare(
				"social_service_dlq",
				true, false, false, false, nil,
			)
			failOnError(err, "social: Failed to declare DLQ")

			err = ch.QueueBind(
				dlq.Name,
				"ride.finished.dlq",
				"events_dlx",
				false,
				nil,
			)
			failOnError(err, "social: Failed to bind DLQ")

			q, err := ch.QueueDeclare(
				"social_service_queue",
				true,
				false,
				false,
				false,
				amqp.Table{
					"x-dead-letter-exchange":    "events_dlx",
					"x-dead-letter-routing-key": "ride.finished.dlq",
				},
			)
			failOnError(err, "social: Failed to declare a queue")

			err = ch.QueueBind(
				q.Name,
				"ride.finished",
				"events_exchange",
				false,
				nil,
			)
			failOnError(err, "social: Failed to bind a queue")

			msgs, err := ch.Consume(
				q.Name,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Println("social: consume failed, retry in 2s:", err)
				ch.Close()
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			log.Println("social: connected to rabbit, consuming messages...")

			for d := range msgs {
				log.Printf("social: Received a message: %s", d.Body)

				log.Println("!!! SAGA ERROR: Simulating processing failure in social-service !!!")

				d.Nack(false, false)
			}

			ch.Close()
			conn.Close()
			log.Println("social: lost rabbit connection, will retry...")
		}
	}()
}

func main() {
	port := getenv("PORT", "8083")
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")

	startConsumer(rabbitURL)

	http.HandleFunc("/feed", feedHandler)

	log.Println("social-service on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
