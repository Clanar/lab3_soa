package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

// newReverseProxy створює новий зворотний проксі для цільового URL
func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	return proxy
}

func main() {
	// Отримуємо URL для кожного сервісу
	// (Ми не використовуємо Service Discovery, тому вказуємо їх вручну, як у ЛР2)
	rideTrackingURL, _ := url.Parse(getenv("RIDE_TRACKING_URL", "http://ride-tracking-service:8082"))
	socialURL, _ := url.Parse(getenv("SOCIAL_URL", "http://social-service:8083"))
	analyticsURL, _ := url.Parse(getenv("ANALYTICS_URL", "http://analytics-service:8084"))
	userURL, _ := url.Parse(getenv("USER_URL", "http://user-service:8081")) // З вашого docker-compose

	// Створюємо проксі для кожного
	rideTrackingProxy := newReverseProxy(rideTrackingURL)
	socialProxy := newReverseProxy(socialURL)
	analyticsProxy := newReverseProxy(analyticsURL)
	userProxy := newReverseProxy(userURL)

	// Головний обробник, який маршрутизує запити
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// /api/ride/finish -> ride-tracking-service:8082/ride/finish
		if strings.HasPrefix(path, "/api/ride/") {
			// Нам потрібно зберегти /ride/finish, а не просто /finish
			// Тому ми модифікуємо URL запиту
			r.URL.Path = strings.TrimPrefix(path, "/api") // -> /ride/finish
			rideTrackingProxy.ServeHTTP(w, r)
			return
		}

		// /api/feed -> social-service:8083/feed
		if strings.HasPrefix(path, "/api/feed") {
			r.URL.Path = strings.TrimPrefix(path, "/api") // -> /feed
			socialProxy.ServeHTTP(w, r)
			return
		}

		// /api/analytics/summary/... -> analytics-service:8084/analytics/summary/...
		if strings.HasPrefix(path, "/api/analytics/") {
			r.URL.Path = strings.TrimPrefix(path, "/api") // -> /analytics/summary/...
			analyticsProxy.ServeHTTP(w, r)
			return
		}

		// /api/users/... -> user-service:8081/users/...
		if strings.HasPrefix(path, "/api/users/") {
			r.URL.Path = strings.TrimPrefix(path, "/api") // -> /users/...
			userProxy.ServeHTTP(w, r)
			return
		}

		// Якщо нічого не збіглося
		http.Error(w, "Not Found", http.StatusNotFound)
	})

	port := getenv("PORT", "8080")
	log.Println("API Gateway on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
