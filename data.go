package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/lib/pq"
)

var parentNameLookup = map[int]string{
	146: "Avery",
	103: "Butler",
	62:  "East Asian Library",
	75:  "John Jay",
	79:  "Lehman LIbrary",
	84:  "Lerner",
	15:  "Mudd",
	2:   "Uris",
}

// dumpFormat represents the datapoints provided for the wireless data.
type dumpFormat struct {
	DumpTime    time.Time
	GroupID     int
	GroupName   string `json:"name"`
	ParentID    int    `json:"parent_id"`
	ParentName  string
	ClientCount int `json:"client_count"`
}

// dataset is an alias for an array of data dumps
type dataset []dumpFormat

// parseData unmarshals a byte array into an array of wireless data dumps.
func parseData(timestamp time.Time, datafile []byte) (dataset, error) {
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
			log.Printf("Failed to parse int, => %s", id)
			continue
		}

		d.GroupID = int(parsedInt)
		d.DumpTime = timestamp

		var exists bool
		d.ParentName, exists = parentNameLookup[d.ParentID]
		if !exists {
			log.Printf("WARN: no parent name for %d exists", d.ParentID)
		}
		data[i] = d
		i++
	}

	return data, nil
}

// insert operates on a list of dumpFormat and inserts them to the provided Postgres
// database.
func (data dataset) insert(db *sql.DB) error {
	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Error starting PG txn => %s", err.Error())
	}

	stmt, err := txn.Prepare(pq.CopyIn(
		"dump_time",
		"group_id",
		"group_name",
		"parent_id",
		"parent_name",
		"client_count",
	))
	if err != nil {
		return fmt.Errorf("Error prepping PG txn => %s", err.Error())
	}
	defer stmt.Close()

	for _, d := range data {
		_, err = stmt.Exec(
			d.DumpTime,
			d.GroupID,
			d.GroupName,
			d.ParentID,
			d.ParentName,
			d.ClientCount,
		)
		if err != nil {
			return fmt.Errorf("Failed to add to bulk insert => %s", err.Error())
		}
	}

	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("Failed to execute bulk insert => %s", err.Error())
	}

	if err = txn.Commit(); err != nil {
		log.Fatalf("Failed to commit txn => %s", err.Error())
	}
	return nil
}
