package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FinishRideRequest struct {
	UserID      string  `json:"userId"`
	DistanceKm  float64 `json:"distanceKm"`
	DurationSec int     `json:"durationSec"`
	AvgSpeedKmh float64 `json:"avgSpeedKmh"`
}

type Ride struct {
	RideID      string  `json:"rideId"`
	UserID      string  `json:"userId"`
	DistanceKm  float64 `json:"distanceKm"`
	DurationSec int     `json:"durationSec"`
	AvgSpeedKmh float64 `json:"avgSpeedKmh"`
	FinishedAt  string  `json:"finishedAt"`
	Status      string  `json:"status"`
}

var db *sql.DB
var rabbitChannel *amqp.Channel

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func initDB() {
	dbSource := getenv("DB_SOURCE", "postgresql://user:password@localhost:5432/ride_db?sslmode=disable")
	var err error

	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", dbSource)
		if err == nil {
			if err = db.Ping(); err == nil {
				log.Println("Connected to PostgreSQL!")
				break
			}
		}
		log.Println("Failed to connect to DB, retrying...", err)
		time.Sleep(2 * time.Second)
	}
	failOnError(err, "Failed to connect to Postgres after retries")

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS rides (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255),
			distance_km FLOAT,
			duration_sec INT,
			avg_speed_kmh FLOAT,
			finished_at VARCHAR(255),
			status VARCHAR(50)
		);
	`)
	failOnError(err, "Failed to create rides table")

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id SERIAL PRIMARY KEY,
			event_type VARCHAR(255) NOT NULL,
			payload JSONB NOT NULL,
			status VARCHAR(50) DEFAULT 'pending',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	failOnError(err, "Failed to create outbox table")
}

func initRabbit() {
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	var conn *amqp.Connection
	var err error

	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			log.Println("Connected to RabbitMQ!")
			break
		}
		log.Println("Failed to connect to RabbitMQ, retrying...", err)
		time.Sleep(2 * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ after retries")

	rabbitChannel, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = rabbitChannel.ExchangeDeclare(
		"events_exchange",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")
}

func finishRideHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FinishRideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
		return
	}

	rideID := "ride_" + time.Now().Format("20060102150405")
	finishedAt := time.Now().UTC().Format(time.RFC3339)

	ride := Ride{
		RideID:      rideID,
		UserID:      req.UserID,
		DistanceKm:  req.DistanceKm,
		DurationSec: req.DurationSec,
		AvgSpeedKmh: req.AvgSpeedKmh,
		FinishedAt:  finishedAt,
		Status:      "finished",
	}

	event := map[string]any{
		"eventType":   "ride.finished",
		"rideId":      ride.RideID,
		"userId":      ride.UserID,
		"distanceKm":  ride.DistanceKm,
		"durationSec": ride.DurationSec,
		"avgSpeedKmh": ride.AvgSpeedKmh,
		"finishedAt":  ride.FinishedAt,
	}
	eventPayload, _ := json.Marshal(event)

	tx, err := db.Begin()
	if err != nil {
		log.Println("Failed to start transaction:", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		INSERT INTO rides (id, user_id, distance_km, duration_sec, avg_speed_kmh, finished_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		ride.RideID, ride.UserID, ride.DistanceKm, ride.DurationSec, ride.AvgSpeedKmh, ride.FinishedAt, ride.Status,
	)
	if err != nil {
		tx.Rollback()
		log.Println("Failed to insert ride:", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		INSERT INTO outbox (event_type, payload)
		VALUES ($1, $2)`,
		event["eventType"],
		eventPayload,
	)
	if err != nil {
		tx.Rollback()
		log.Println("Failed to insert into outbox:", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Println("Failed to commit transaction:", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"rideId":  rideID,
		"message": "Ride finished and event queued in outbox",
	})
}

func startMessageRelay() {
	log.Println("Message Relay started...")
	for {
		time.Sleep(5 * time.Second)

		rows, err := db.Query("SELECT id, event_type, payload FROM outbox WHERE status = 'pending' ORDER BY created_at ASC LIMIT 10")
		if err != nil {
			log.Printf("Relay: Error querying outbox: %v", err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var id int
			var eventType string
			var payload []byte

			if err := rows.Scan(&id, &eventType, &payload); err != nil {
				log.Printf("Relay: Error scanning row: %v", err)
				continue
			}

			err := rabbitChannel.Publish(
				"events_exchange",
				eventType,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        payload,
				},
			)

			if err == nil {
				log.Printf("Relay: Successfully published event ID %d", id)
				_, updateErr := db.Exec("UPDATE outbox SET status = 'sent' WHERE id = $1", id)
				if updateErr != nil {
					log.Printf("Relay: CRITICAL! Failed to update outbox status for ID %d: %v", id, updateErr)
				}
			} else {
				log.Printf("Relay: Failed to publish event ID %d: %v. Will retry.", id, err)
				break
			}
		}
	}
}

func startCompensationConsumer(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"ride_tracking_compensation_queue",
		true, false, false, false, nil,
	)
	failOnError(err, "Failed to declare compensation queue")

	err = ch.QueueBind(
		q.Name,
		"ride.finish.failed",
		"events_exchange",
		false, nil,
	)
	failOnError(err, "Failed to bind compensation queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register compensation consumer")

	go func() {
		log.Println("Compensation Consumer (C1) started...")
		for d := range msgs {
			log.Printf("COMPENSATION (C1): Received 'ride.finish.failed': %s", d.Body)

			var event map[string]any
			if err := json.Unmarshal(d.Body, &event); err == nil {
				rideID := event["rideId"].(string)

				_, err := db.Exec("UPDATE rides SET status = 'failed' WHERE id = $1", rideID)
				if err != nil {
					log.Printf("COMPENSATION (C1): FAILED to roll back ride status: %v", err)
				} else {
					log.Printf("COMPENSATION (C1): Rolled back ride status to 'failed' for ride: %s", rideID)
				}
			}
		}
	}()
}

func main() {
	initDB()
	defer db.Close()

	initRabbit()
	defer rabbitChannel.Close()

	go startMessageRelay()

	go startCompensationConsumer(rabbitChannel)

	http.HandleFunc("/ride/finish", finishRideHandler)

	port := getenv("PORT", "8082")
	log.Println("ride-tracking-service on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
