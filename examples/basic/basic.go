package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/segmentio/ctlstore"
)

var database = []book{}

type book struct {
	ID    []byte `json:"id"`
	ISBN  string `json:"isbn"`
	Title string `json:"title"`
}

type createBookRequest struct {
	ISBN  string `json:"isbn"`
	Title string `json:"title"`
}

type listBooksResponse struct {
	Books []book `json:"books"`
}

type ctlstoreMutateRequest struct {
	Table  string       `json:"table"`
	Values ctlstoreBook `json:"values"`
}

type ctlstoreBook struct {
	ID    []byte `ctlstore:"id"`
	ISBN  string `ctlstore:"isbn"`
	Title string `ctlstore:"title"`
}

const executiveURL = "http://localhost:3000"
const family = "store"
const table = "books"
const writer = "store"
const writeSecret = "abcdef"
const cookie = "mycookie"

func main() {
	// Create the family
	http.Post(fmt.Sprintf("%s/families/%s", executiveURL, family), "application/json", nil)

	// Create the writer
	http.Post(fmt.Sprintf("%s/writers/%s", executiveURL, family), "application/json", strings.NewReader(writeSecret))

	// Create the table
	http.Post(fmt.Sprintf("%s/families/%s/tables/%s", executiveURL, family, table), "application/json", bytes.NewReader([]byte(`
	{
		"fields": [
			[
				"id",
				"bytestring"
			],
			[
				"isbn",
				"text"
			],
			[
				"title",
				"string"
			]
		],
		"keyFields": [
			"id"
		]
	}
`)))

	// Create the Cookie
	client := http.Client{}
	cReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/cookie", executiveURL), strings.NewReader(cookie))
	cReq.Header.Add("ctlstore-writer", writer)
	cReq.Header.Add("ctlstore-secret", writeSecret)
	client.Do(cReq)

	// Spin up the server
	router := httprouter.New()
	router.POST("/v1/books", createBook)
	router.GET("/v1/books", listBooks)
	log.Fatal(http.ListenAndServe(":8080", router))
}

func createBook(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var req createBookRequest
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&req); err != nil {
		w.WriteHeader(400)
	}

	id, _ := base64.StdEncoding.DecodeString(uuid.New().String())

	b := book{
		ID:    id,
		ISBN:  req.ISBN,
		Title: req.Title,
	}

	// Write to the "database"
	database = append(database, b)

	// Write to ctlstore via Mutate endpoint
	go func() {
		mutation := ctlstoreMutateRequest{
			Table: "books",
			Values: ctlstoreBook{
				ID:    []byte(b.ID),
				ISBN:  b.ISBN,
				Title: b.Title,
			},
		}
		mutateReq := struct {
			Cookie    string                  `json:"cookie"`
			Mutations []ctlstoreMutateRequest `json:"mutations"`
		}{
			Cookie:    "Na==",
			Mutations: []ctlstoreMutateRequest{mutation},
		}

		jd, _ := json.Marshal(mutateReq)

		client := http.Client{}

		mReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/families/%s/mutations", executiveURL, family), bytes.NewBuffer(jd))
		mReq.Header.Add("Content-Type", "application/json")
		mReq.Header.Add("ctlstore-writer", writer)
		mReq.Header.Add("ctlstore-secret", writeSecret)
		client.Do(mReq)
	}()

	// Send the response
	encoder := json.NewEncoder(w)
	encoder.Encode(b)
}

func listBooks(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Oops, some foolish engineer accidently dropped the DB
	database = []book{}

	// But we read from ctlstore, so this endpoint still works
	ldbReader, _ := ctlstore.Reader()

	rows, _ := ldbReader.GetRowsByKeyPrefix(r.Context(), "store", "books")

	books := []book{}
	for rows.Next() {
		var cbook ctlstoreBook
		if err := rows.Scan(&cbook); err != nil {
			log.Fatal(err)
		}

		books = append(books, book{
			ID:    cbook.ID,
			ISBN:  cbook.ISBN,
			Title: cbook.Title,
		})

	}

	resp := listBooksResponse{
		Books: books,
	}

	encoder := json.NewEncoder(w)
	encoder.Encode(resp)
}
