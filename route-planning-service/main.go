package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Route struct {
	RouteID   string    `json:"routeId"`
	UserID    string    `json:"userId"`
	Waypoints []string  `json:"waypoints"`
	Privacy   string    `json:"privacy"`
	CreatedAt time.Time `json:"createdAt"`
}

var (
	routes = map[string]Route{}
	mu     sync.Mutex
)

func createRouteHandler(w http.ResponseWriter, r *http.Request) {
	type Req struct {
		UserID    string   `json:"userId"`
		Waypoints []string `json:"waypoints"`
		Privacy   string   `json:"privacy"`
	}
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
		return
	}

	id := "route_" + time.Now().Format("20060102150405")

	route := Route{
		RouteID:   id,
		UserID:    req.UserID,
		Waypoints: req.Waypoints,
		Privacy:   req.Privacy,
		CreatedAt: time.Now().UTC(),
	}

	mu.Lock()
	routes[id] = route
	mu.Unlock()

	json.NewEncoder(w).Encode(map[string]any{
		"status":  "created",
		"routeId": id,
	})
}

func getRouteHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/routes/"):]
	mu.Lock()
	route, ok := routes[id]
	mu.Unlock()

	if !ok {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(route)
}

func main() {
	port := getenv("PORT", "8087")

	http.HandleFunc("/routes/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			getRouteHandler(w, r)
		} else if r.Method == http.MethodPost && r.URL.Path == "/routes/" {
			createRouteHandler(w, r)
		} else {
			http.NotFound(w, r)
		}
	})

	log.Println("route-planning-service on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
