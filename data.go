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

// lookup table for buildings that we have in our system.
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
//
// This is a little more complicated because the group ID is stored as the key to the
// remainder of the data for the record.
func parseData(timestamp time.Time, datafile []byte) (dataset, error) {
	// marshal what data we can from the json
	parsed := make(map[string]dumpFormat)
	if err := json.Unmarshal(datafile, &parsed); err != nil {
		return []dumpFormat{}, fmt.Errorf("Error parsing bytes => %s", err.Error())
	}

	var (
		data   []dumpFormat = make([]dumpFormat, len(parsed))
		i      int          = 0
		err    error
		exists bool
	)
	// fill out the JSON with the group ID added
	for id, d := range parsed {
		var parsedInt int64
		if parsedInt, err = strconv.ParseInt(id, 10, 64); err != nil {
			log.Fatalf("Failed to parse int, %s => %s", id, err.Error())
		}

		d.GroupID = int(parsedInt)
		d.DumpTime = timestamp
		if d.ParentName, exists = parentNameLookup[d.ParentID]; !exists {
			log.Printf("WARN: no parent name for %d exists in group: %d", d.ParentID, d.GroupID)
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

	// CopyIn used for fast insertions. Table followed by columns
	stmt, err := txn.Prepare(pq.CopyIn(
		"density_data",
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
