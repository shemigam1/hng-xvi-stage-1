package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
	db         *sql.DB
	httpClient = &http.Client{Timeout: 15 * time.Second}
)

// ─── Models ───────────────────────────────────────────────────────────────────

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

// ─── External API types ───────────────────────────────────────────────────────

type genderizeResponse struct {
	Gender      *string `json:"gender"`
	Probability float64 `json:"probability"`
	Count       int     `json:"count"`
}

type agifyResponse struct {
	Age   *int `json:"age"`
	Count int  `json:"count"`
}

type nationalizeCountry struct {
	CountryID   string  `json:"country_id"`
	Probability float64 `json:"probability"`
}

type nationalizeResponse struct {
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

// corsMiddleware wraps every handler and guarantees CORS headers are always present,
// regardless of what the inner handler does (including panics / early returns).
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// ─── External API calls (concurrent, with retry) ─────────────────────────────

func fetchJSON(ctx context.Context, rawURL string, dest any) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(300 * time.Millisecond)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "HNG-Stage1-Bot/1.0")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("upstream status %d", resp.StatusCode)
			continue
		}
		if err := json.Unmarshal(body, dest); err != nil {
			return err
		}
		return nil
	}
	return lastErr
}

type externalResults struct {
	gender      *genderizeResponse
	agify       *agifyResponse
	nationalize *nationalizeResponse
	errSrc      string // which API failed
	err         error
}

// callAllAPIs fires all three external calls concurrently.
func callAllAPIs(ctx context.Context, name string) externalResults {
	encoded := url.QueryEscape(name)

	var (
		mu  sync.Mutex
		res externalResults
		wg  sync.WaitGroup
	)

	type job struct {
		url    string
		apiKey string
		decode func([]byte) error
	}

	var gData genderizeResponse
	var aData agifyResponse
	var nData nationalizeResponse

	jobs := []job{
		{
			url:    "https://api.genderize.io?name=" + encoded,
			apiKey: "Genderize",
			decode: func(b []byte) error { return json.Unmarshal(b, &gData) },
		},
		{
			url:    "https://api.agify.io?name=" + encoded,
			apiKey: "Agify",
			decode: func(b []byte) error { return json.Unmarshal(b, &aData) },
		},
		{
			url:    "https://api.nationalize.io?name=" + encoded,
			apiKey: "Nationalize",
			decode: func(b []byte) error { return json.Unmarshal(b, &nData) },
		},
	}

	for _, j := range jobs {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := fetchJSON(ctx, j.url, j.decode)
			if err == nil {
				// run the decode against the raw struct pointer
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if res.err == nil {
				res.errSrc = j.apiKey
				res.err = err
			}
		}()
	}
	wg.Wait()

	// fetchJSON already decoded into the struct pointers via the decode func.
	// But we passed a func that re-unmarshals — let's just do a direct fetch instead.
	// (See simpler parallel approach below — we'll keep a cleaner pattern.)
	res.gender = &gData
	res.agify = &aData
	res.nationalize = &nData
	return res
}

// callAPIsParallel is the cleaner parallel implementation.
func callAPIsParallel(ctx context.Context, name string) (
	g *genderizeResponse, a *agifyResponse, n *nationalizeResponse,
	failedAPI string, err error,
) {
	encoded := url.QueryEscape(name)

	type gResult struct {
		data *genderizeResponse
		err  error
	}
	type aResult struct {
		data *agifyResponse
		err  error
	}
	type nResult struct {
		data *nationalizeResponse
		err  error
	}

	gCh := make(chan gResult, 1)
	aCh := make(chan aResult, 1)
	nCh := make(chan nResult, 1)

	go func() {
		var r genderizeResponse
		e := fetchJSON(ctx, "https://api.genderize.io?name="+encoded, &r)
		gCh <- gResult{&r, e}
	}()
	go func() {
		var r agifyResponse
		e := fetchJSON(ctx, "https://api.agify.io?name="+encoded, &r)
		aCh <- aResult{&r, e}
	}()
	go func() {
		var r nationalizeResponse
		e := fetchJSON(ctx, "https://api.nationalize.io?name="+encoded, &r)
		nCh <- nResult{&r, e}
	}()

	gr := <-gCh
	ar := <-aCh
	nr := <-nCh

	if gr.err != nil {
		return nil, nil, nil, "Genderize", gr.err
	}
	if ar.err != nil {
		return nil, nil, nil, "Agify", ar.err
	}
	if nr.err != nil {
		return nil, nil, nil, "Nationalize", nr.err
	}
	return gr.data, ar.data, nr.data, "", nil
}

// ─── Handlers ────────────────────────────────────────────────────────────────

// POST /api/profiles
func handleCreateProfile(w http.ResponseWriter, r *http.Request) {
	// Accept only JSON body with "name" key.
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

	// Idempotency: return existing profile if the name is already stored.
	var existing Profile
	err := db.QueryRowContext(r.Context(), `
		SELECT id, name, gender, gender_probability, sample_size,
		       age, age_group, country_id, country_probability, created_at
		FROM profiles WHERE LOWER(name) = LOWER($1)`, name).
		Scan(&existing.ID, &existing.Name, &existing.Gender,
			&existing.GenderProbability, &existing.SampleSize,
			&existing.Age, &existing.AgeGroup,
			&existing.CountryID, &existing.CountryProbability, &existing.CreatedAt)
	if err == nil {
		existing.CreatedAt = existing.CreatedAt.UTC()
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":  "success",
			"message": "Profile already exists",
			"data":    existing,
		})
		return
	}
	if err != sql.ErrNoRows {
		log.Printf("DB lookup error: %v", err)
		errJSON(w, http.StatusInternalServerError, "Database error")
		return
	}

	// Call all three external APIs concurrently (with 12s deadline).
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()

	gData, aData, nData, failedAPI, apiErr := callAPIsParallel(ctx, name)
	if apiErr != nil {
		log.Printf("External API error (%s): %v", failedAPI, apiErr)
		errJSON(w, http.StatusBadGateway, failedAPI+" returned an invalid response")
		return
	}

	// Validate responses.
	if gData.Gender == nil || gData.Count == 0 {
		errJSON(w, http.StatusBadGateway, "Genderize returned an invalid response")
		return
	}
	if aData.Age == nil {
		errJSON(w, http.StatusBadGateway, "Agify returned an invalid response")
		return
	}
	if len(nData.Country) == 0 {
		errJSON(w, http.StatusBadGateway, "Nationalize returned an invalid response")
		return
	}

	// Pick the country with the highest probability.
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

	_, err = db.ExecContext(r.Context(), `
		INSERT INTO profiles
		  (id, name, gender, gender_probability, sample_size,
		   age, age_group, country_id, country_probability, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		profile.ID, profile.Name, profile.Gender, profile.GenderProbability,
		profile.SampleSize, profile.Age, profile.AgeGroup,
		profile.CountryID, profile.CountryProbability, profile.CreatedAt)
	if err != nil {
		log.Printf("DB insert error: %v", err)
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
	err := db.QueryRowContext(r.Context(), `
		SELECT id, name, gender, gender_probability, sample_size,
		       age, age_group, country_id, country_probability, created_at
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
	p.CreatedAt = p.CreatedAt.UTC()
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "success",
		"data":   p,
	})
}

// GET /api/profiles
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

	rows, err := db.QueryContext(r.Context(), query, args...)
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
	result, err := db.ExecContext(r.Context(), `DELETE FROM profiles WHERE id = $1`, id)
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

// ─── Routing ─────────────────────────────────────────────────────────────────

// profilesCollection handles /api/profiles (exact, no trailing slash).
func profilesCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleListProfiles(w, r)
	case http.MethodPost:
		handleCreateProfile(w, r)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// profilesItem handles /api/profiles/{id}.
func profilesItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/profiles/")
	id = strings.Trim(id, "/")

	if id == "" {
		// Trailing-slash variant of the collection — treat as collection.
		profilesCollection(w, r)
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

// ─── DB ──────────────────────────────────────────────────────────────────────

func initDB() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("db.Ping: %v", err)
	}
	log.Println("Connected to database")

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

	_, err = db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS profiles_name_lower
		ON profiles (LOWER(name))`)
	if err != nil {
		log.Fatalf("CREATE INDEX: %v", err)
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found, falling back to environment variables")
	}

	initDB()

	mux := http.NewServeMux()
	// Wrap every route with CORS middleware so headers are ALWAYS present.
	mux.HandleFunc("/api/profiles", corsMiddleware(profilesCollection))
	mux.HandleFunc("/api/profiles/", corsMiddleware(profilesItem))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
