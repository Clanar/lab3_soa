package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Stats struct {
	TotalKm float64 `json:"totalKm"`
}

var (
	stats = map[string]*Stats{}
	mu    sync.Mutex
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
	userId := strings.TrimPrefix(r.URL.Path, "/analytics/summary/")
	mu.Lock()
	s, ok := stats[userId]
	mu.Unlock()
	if !ok {
		s = &Stats{TotalKm: 0.0}
	}
	json.NewEncoder(w).Encode(map[string]any{
		"userId":  userId,
		"totalKm": s.TotalKm,
	})
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func startConsumer(rabbitURL string) {
	go func() {
		for {
			conn, err := amqp.Dial(rabbitURL)
			if err != nil {
				log.Println("analytics (T2): cannot connect rabbit:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Println("analytics (T2): cannot open channel:", err)
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			err = ch.ExchangeDeclare(
				"events_exchange", "topic", true, false, false, false, nil,
			)
			if err != nil {
				log.Println("analytics (T2): declare exchange failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			q, err := ch.QueueDeclare(
				"analytics_service_queue",
				true, false, false, false, nil,
			)
			if err != nil {
				log.Println("analytics (T2): queue declare failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			err = ch.QueueBind(
				q.Name, "ride.finished", "events_exchange", false, nil,
			)
			if err != nil {
				log.Println("analytics (T2): queue bind failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
			if err != nil {
				log.Println("analytics (T2): consume failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			log.Println("analytics: (T2) connected, consuming 'ride.finished' messages...")

			for d := range msgs {
				var evt map[string]any
				if err := json.Unmarshal(d.Body, &evt); err == nil {
					if eventType, ok := evt["eventType"].(string); ok && eventType == "ride.finished" {
						userId := evt["userId"].(string)
						dist := evt["distanceKm"].(float64)
						mu.Lock()
						if _, ok := stats[userId]; !ok {
							stats[userId] = &Stats{TotalKm: 0}
						}
						stats[userId].TotalKm += dist
						mu.Unlock()
						log.Println("analytics (T2): updated stats for", userId, "now", stats[userId].TotalKm)
					}
				} else {
					log.Println("analytics (T2): failed to parse message:", err)
				}
				d.Ack(false)
			}
			ch.Close()
			conn.Close()
			log.Println("analytics (T2): lost rabbit connection, will retry...")
		}
	}()
}

func startDLQConsumer(rabbitURL string) {
	go func() {
		for {
			conn, err := amqp.Dial(rabbitURL)
			if err != nil {
				log.Println("analytics (DLQ): cannot connect rabbit:", err)
				time.Sleep(5 * time.Second)
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Println("analytics (DLQ): cannot open channel:", err)
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			err = ch.ExchangeDeclare("events_dlx", "direct", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (DLQ): declare dlx failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}
			err = ch.ExchangeDeclare("events_exchange", "topic", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (DLQ): declare exchange failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			q, err := ch.QueueDeclare("social_service_dlq", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (DLQ): declare queue failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			err = ch.QueueBind(q.Name, "ride.finished.dlq", "events_dlx", false, nil)
			if err != nil {
				log.Println("analytics (DLQ): queue bind failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			msgs, err := ch.Consume(q.Name, "dlq_consumer", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (DLQ): consume failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			log.Println("analytics: (Trigger) DLQ Consumer started. Waiting for failed messages...")

			for d := range msgs {
				log.Printf("DLQ Consumer (Trigger): Received failed message: %s", d.Body)
				err := ch.Publish(
					"events_exchange",
					"ride.finish.failed",
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        d.Body,
					},
				)
				if err != nil {
					log.Printf("DLQ Consumer (Trigger): FAILED to publish compensation event: %v", err)
				} else {
					log.Println("DLQ Consumer (Trigger): Published 'ride.finish.failed' event.")
				}
			}
			ch.Close()
			conn.Close()
			log.Println("analytics (DLQ): lost rabbit connection, will retry...")
		}
	}()
}

func startCompensationConsumer(rabbitURL string) {
	go func() {
		for {
			conn, err := amqp.Dial(rabbitURL)
			if err != nil {
				log.Println("analytics (C2): cannot connect rabbit:", err)
				time.Sleep(5 * time.Second)
				continue
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Println("analytics (C2): cannot open channel:", err)
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			err = ch.ExchangeDeclare("events_exchange", "topic", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (C2): declare exchange failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			q, err := ch.QueueDeclare("analytics_compensation_queue", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (C2): declare queue failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			err = ch.QueueBind(q.Name, "ride.finish.failed", "events_exchange", false, nil)
			if err != nil {
				log.Println("analytics (C2): queue bind failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			msgs, err := ch.Consume(q.Name, "c2_consumer", true, false, false, false, nil)
			if err != nil {
				log.Println("analytics (C2): consume failed:", err)
				ch.Close()
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			log.Println("analytics: (C2) Compensation consumer started...")

			for d := range msgs {
				log.Printf("COMPENSATION (C2): Received 'ride.finish.failed': %s", d.Body)
				var evt map[string]any
				if err := json.Unmarshal(d.Body, &evt); err == nil {
					userId := evt["userId"].(string)
					dist := evt["distanceKm"].(float64)
					mu.Lock()
					if s, ok := stats[userId]; ok {
						s.TotalKm -= dist
						log.Println("COMPENSATION (C2): Rolled back stats for", userId, "now", s.TotalKm)
					}
					mu.Unlock()
				}
			}
			ch.Close()
			conn.Close()
			log.Println("analytics (C2): lost rabbit connection, will retry...")
		}
	}()
}

func main() {
	port := getenv("PORT", "8084")
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")

	startConsumer(rabbitURL)
	startDLQConsumer(rabbitURL)
	startCompensationConsumer(rabbitURL)

	http.HandleFunc("/analytics/summary/", summaryHandler)

	log.Println("analytics-service on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
