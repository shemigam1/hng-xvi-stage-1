package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var db *sql.DB

// Profile is the full profile stored in DB and returned for single/create endpoints.
type Profile struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	Gender             string    `json:"gender"`
	GenderProbability  float64   `json:"gender_probability"`
	SampleSize         int       `json:"sample_size"`
	Age                int       `json:"age"`
	AgeGroup           string    `json:"age_group"`
	CountryID          string    `json:"country_id"`
	CountryProbability float64   `json:"country_probability"`
	CreatedAt          time.Time `json:"created_at"`
}

// ProfileSummary is the trimmed view returned by the list endpoint.
type ProfileSummary struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Gender    string `json:"gender"`
	Age       int    `json:"age"`
	AgeGroup  string `json:"age_group"`
	CountryID string `json:"country_id"`
}

// External API response types.
type genderizeResponse struct {
	Name        string  `json:"name"`
	Gender      *string `json:"gender"`
	Probability float64 `json:"probability"`
	Count       int     `json:"count"`
}

type agifyResponse struct {
	Name  string `json:"name"`
	Age   *int   `json:"age"`
	Count int    `json:"count"`
}

type nationalizeCountry struct {
	CountryID   string  `json:"country_id"`
	Probability float64 `json:"probability"`
}

type nationalizeResponse struct {
	Name    string               `json:"name"`
	Country []nationalizeCountry `json:"country"`
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func classifyAge(age int) string {
	switch {
	case age <= 12:
		return "child"
	case age <= 19:
		return "teenager"
	case age <= 59:
		return "adult"
	default:
		return "senior"
	}
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

func errJSON(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"status":  "error",
		"message": message,
	})
}

// ─── External API calls ───────────────────────────────────────────────────────

func fetchJSON(rawURL string, dest any) error {
	resp, err := http.Get(rawURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, dest)
}

func callGenderize(name string) (*genderizeResponse, error) {
	var r genderizeResponse
	if err := fetchJSON("https://api.genderize.io?name="+url.QueryEscape(name), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func callAgify(name string) (*agifyResponse, error) {
	var r agifyResponse
	if err := fetchJSON("https://api.agify.io?name="+url.QueryEscape(name), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func callNationalize(name string) (*nationalizeResponse, error) {
	var r nationalizeResponse
	if err := fetchJSON("https://api.nationalize.io?name="+url.QueryEscape(name), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ─── Handlers ────────────────────────────────────────────────────────────────

// POST /api/profiles
func handleCreateProfile(w http.ResponseWriter, r *http.Request) {
	// Parse body: keep name as interface{} so we can detect wrong types.
	var raw map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		errJSON(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	nameVal, exists := raw["name"]
	if !exists || nameVal == nil {
		errJSON(w, http.StatusBadRequest, "Missing or empty name")
		return
	}

	nameStr, ok := nameVal.(string)
	if !ok {
		errJSON(w, http.StatusUnprocessableEntity, "Invalid type")
		return
	}

	name := strings.TrimSpace(nameStr)
	if name == "" {
		errJSON(w, http.StatusBadRequest, "Missing or empty name")
		return
	}

	// Idempotency: return existing profile if name already exists (case-insensitive).
	var existing Profile
	err := db.QueryRow(`
		SELECT id, name, gender, gender_probability, sample_size, age, age_group,
		       country_id, country_probability, created_at
		FROM profiles WHERE LOWER(name) = LOWER($1)`, name).
		Scan(&existing.ID, &existing.Name, &existing.Gender, &existing.GenderProbability,
			&existing.SampleSize, &existing.Age, &existing.AgeGroup,
			&existing.CountryID, &existing.CountryProbability, &existing.CreatedAt)
	if err == nil {
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":  "success",
			"message": "Profile already exists",
			"data":    existing,
		})
		return
	}
	if err != sql.ErrNoRows {
		errJSON(w, http.StatusInternalServerError, "Database error")
		return
	}

	// Call all three external APIs.
	gData, err := callGenderize(name)
	if err != nil || gData.Gender == nil || gData.Count == 0 {
		errJSON(w, http.StatusBadGateway, "Genderize returned an invalid response")
		return
	}

	aData, err := callAgify(name)
	if err != nil || aData.Age == nil {
		errJSON(w, http.StatusBadGateway, "Agify returned an invalid response")
		return
	}

	nData, err := callNationalize(name)
	if err != nil || len(nData.Country) == 0 {
		errJSON(w, http.StatusBadGateway, "Nationalize returned an invalid response")
		return
	}

	// Pick highest-probability country.
	top := nData.Country[0]
	for _, c := range nData.Country[1:] {
		if c.Probability > top.Probability {
			top = c
		}
	}

	// Generate UUID v7.
	id, err := uuid.NewV7()
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "Failed to generate ID")
		return
	}

	profile := Profile{
		ID:                 id.String(),
		Name:               name,
		Gender:             *gData.Gender,
		GenderProbability:  gData.Probability,
		SampleSize:         gData.Count,
		Age:                *aData.Age,
		AgeGroup:           classifyAge(*aData.Age),
		CountryID:          top.CountryID,
		CountryProbability: top.Probability,
		CreatedAt:          time.Now().UTC(),
	}

	_, err = db.Exec(`
		INSERT INTO profiles
		  (id, name, gender, gender_probability, sample_size, age, age_group,
		   country_id, country_probability, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		profile.ID, profile.Name, profile.Gender, profile.GenderProbability,
		profile.SampleSize, profile.Age, profile.AgeGroup,
		profile.CountryID, profile.CountryProbability, profile.CreatedAt)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "Failed to store profile")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"status": "success",
		"data":   profile,
	})
}

// GET /api/profiles/:id
func handleGetProfile(w http.ResponseWriter, r *http.Request, id string) {
	var p Profile
	err := db.QueryRow(`
		SELECT id, name, gender, gender_probability, sample_size, age, age_group,
		       country_id, country_probability, created_at
		FROM profiles WHERE id = $1`, id).
		Scan(&p.ID, &p.Name, &p.Gender, &p.GenderProbability, &p.SampleSize,
			&p.Age, &p.AgeGroup, &p.CountryID, &p.CountryProbability, &p.CreatedAt)
	if err == sql.ErrNoRows {
		errJSON(w, http.StatusNotFound, "Profile not found")
		return
	}
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "Database error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "success",
		"data":   p,
	})
}

// GET /api/profiles  (with optional filter params)
func handleListProfiles(w http.ResponseWriter, r *http.Request) {
	query := `
		SELECT id, name, gender, age, age_group, country_id
		FROM profiles WHERE 1=1`
	args := []any{}
	idx := 1

	if g := r.URL.Query().Get("gender"); g != "" {
		query += fmt.Sprintf(" AND LOWER(gender) = LOWER($%d)", idx)
		args = append(args, g)
		idx++
	}
	if c := r.URL.Query().Get("country_id"); c != "" {
		query += fmt.Sprintf(" AND LOWER(country_id) = LOWER($%d)", idx)
		args = append(args, c)
		idx++
	}
	if ag := r.URL.Query().Get("age_group"); ag != "" {
		query += fmt.Sprintf(" AND LOWER(age_group) = LOWER($%d)", idx)
		args = append(args, ag)
		idx++
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	profiles := []ProfileSummary{}
	for rows.Next() {
		var p ProfileSummary
		if err := rows.Scan(&p.ID, &p.Name, &p.Gender, &p.Age, &p.AgeGroup, &p.CountryID); err != nil {
			errJSON(w, http.StatusInternalServerError, "Database error")
			return
		}
		profiles = append(profiles, p)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "success",
		"count":  len(profiles),
		"data":   profiles,
	})
}

// DELETE /api/profiles/:id
func handleDeleteProfile(w http.ResponseWriter, r *http.Request, id string) {
	result, err := db.Exec(`DELETE FROM profiles WHERE id = $1`, id)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "Database error")
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		errJSON(w, http.StatusNotFound, "Profile not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Router ───────────────────────────────────────────────────────────────────

// /api/profiles  — collection
func profilesCollection(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet:
		handleListProfiles(w, r)
	case http.MethodPost:
		handleCreateProfile(w, r)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// /api/profiles/:id  — single item
func profilesItem(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Extract ID: strip "/api/profiles/" prefix.
	id := strings.TrimPrefix(r.URL.Path, "/api/profiles/")
	id = strings.Trim(id, "/")
	if id == "" {
		errJSON(w, http.StatusBadRequest, "Missing profile ID")
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGetProfile(w, r, id)
	case http.MethodDelete:
		handleDeleteProfile(w, r, id)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// ─── DB init ──────────────────────────────────────────────────────────────────

func initDB() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost/hng_profiles?sslmode=disable"
	}

	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("db.Ping: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS profiles (
			id                  TEXT PRIMARY KEY,
			name                TEXT NOT NULL,
			gender              TEXT NOT NULL,
			gender_probability  DOUBLE PRECISION NOT NULL,
			sample_size         INTEGER NOT NULL,
			age                 INTEGER NOT NULL,
			age_group           TEXT NOT NULL,
			country_id          TEXT NOT NULL,
			country_probability DOUBLE PRECISION NOT NULL,
			created_at          TIMESTAMPTZ NOT NULL
		)`)
	if err != nil {
		log.Fatalf("CREATE TABLE: %v", err)
	}

	// Unique index on lowercase name for idempotency.
	_, err = db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS profiles_name_lower
		ON profiles (LOWER(name))`)
	if err != nil {
		log.Fatalf("CREATE INDEX: %v", err)
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	// Load .env if present (silently ignored in production where env vars are set directly).
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found, using environment variables")
	}

	initDB()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/profiles", profilesCollection)
	mux.HandleFunc("/api/profiles/", profilesItem)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
