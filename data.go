package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/lib/pq"
)

// dumpFormat represents the datapoints provided for the wireless data.
type dumpFormat struct {
	ID          int
	Name        string `json:"name"`
	ClientCount int    `json:"client_count"`
	ParentID    int    `json:"parent_id"`
}

// parseData unmarshals a byte array into an array of wireless data dumps.
func parseData(datafile []byte) ([]dumpFormat, error) {
	var parsed map[string]dumpFormat = make(map[string]dumpFormat)
	err := json.Unmarshal(datafile, &parsed)
	if err != nil {
		return []dumpFormat{}, fmt.Errorf("Error parsing bytes => %s", err.Error())
	}

	var data []dumpFormat = make([]dumpFormat, len(parsed))
	var i int = 0
	for id, d := range parsed {
		var parsedInt int64
		if parsedInt, err = strconv.ParseInt(id, 10, 64); err != nil {
			log.Print("Failed to parse int, => %s", id)
			continue
		}

		d.ID = int(parsedInt)
		data[i] = d
		i++
	}

	return data, nil
}

// dataset is an alias for an array of data dumps
type dataset []dumpFormat

// insert operates on a list of dumpFormat and inserts them to the provided Postgres
// database.
func (data dataset) insert(db *sql.DB) error {
	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Error starting PG txn => %s", err.Error())
	}

	stmt, err := txn.Prepare(pq.CopyIn("name", "count"))
	if err != nil {
		return fmt.Errorf("Error prepping PG txn => %s", err.Error())
	}
	defer stmt.Close()

	for _, d := range data {
		_, err = stmt.Exec(d.Name, d.ClientCount)
		if err != nil {
			return fmt.Errorf("Failed to add to bulk insert => %s", err.Error())
		}
	}

	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("Failed to execute bulk insert => %s", err.Error())
	}

	if err = txn.Commit(); err != nil {
		log.Fatal("Failed to commit txn => %s", err.Error())
	}
	return nil
}
