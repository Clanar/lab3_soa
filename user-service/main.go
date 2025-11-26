package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

type User struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

var users = map[string]User{
	"u42": {ID: "u42", Name: "Yehor", Status: "active"},
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/users/"):]
	u, ok := users[id]
	if !ok {
		http.Error(w, `{"error":"user not found"}`, http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(u)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	http.HandleFunc("/users/", getUserHandler)

	log.Println("user-service on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
