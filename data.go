package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adicu/wireless_data_processor/Godeps/_workspace/src/github.com/lib/pq"
)

// lookup table for buildings that we have in our system.
var parentNameLookup = map[int]string{
	146: "Avery",
	103: "Butler",
	62:  "East Asian Library",
	75:  "John Jay",
	79:  "Lehman Library",
	84:  "Lerner",
	15:  "Northwest Corner Building",
	2:   "Uris",
}

// dataset is an alias for an array of data dumps
type dataset []dumpFormat

// dumpFormat represents the datapoints provided for the wireless data.
// DumpTime, GroupID, & ParentName are gathered from the dumped JSON file.
// GroupName, ParentID, & ClientCount are configured based on the filename and
// JSON format.
type dumpFormat struct {
	DumpTime    time.Time
	GroupID     int
	ParentName  string
	GroupName   string
	ParentID    int
	ClientCount int
}

// UnmarshalJSON inmplements JSON's Unmarshaler interface.
// This allows us to deal with inconsistent number encoding in the 'parent_id' and
// 'client_count' fields.
func (df *dumpFormat) UnmarshalJSON(data []byte) error {
	// pull data into a generic map
	raw := make(map[string]interface{})
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return fmt.Errorf("Failed to unpack data into map => {%s}", err)
	}

	// get name and insure it's a string
	name, exists := raw["name"]
	if !exists {
		return fmt.Errorf("key 'name' missing ")
	}
	switch n := name.(type) {
	case string:
		df.GroupName = n
	default:
		return fmt.Errorf("Value in 'name' should be string")
	}

	// get parent_id and take either int or string
	pID, exists := raw["parent_id"]
	if !exists {
		return fmt.Errorf("key 'parent_id' missing ")
	}
	switch p := pID.(type) {
	case string:
		i, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("Failed to convert 'parent_id', %s, to int => {%s}", p, err)
		}
		df.ParentID = i
	case float64:
		df.ParentID = int(p)
	default:
		return fmt.Errorf("Value in 'parent_id', %#v, should be int or string", p)
	}

	// get client_count and take either int or string
	cc, exists := raw["client_count"]
	if !exists {
		return fmt.Errorf("key 'client_count' missing ")
	}
	switch c := cc.(type) {
	case string:
		i, err := strconv.Atoi(c)
		if err != nil {
			return fmt.Errorf("Failed to convert 'client_count', %s, to int => {%s}", c, err)
		}
		df.ClientCount = i
	case float64:
		df.ClientCount = int(c)
	default:
		return fmt.Errorf("Value in 'client_count' should be int or string")
	}

	return nil
}

// parseData unmarshals a byte array into an array of wireless data dumps.
//
// This is a little more complicated because the group ID is stored as the key to the
// remainder of the data for the record.
//
// adds:
// - a timestamp based on the filename
// - a group ID based on the group's key in the JSON
// - a parent name based on the parentNameLookup table
func parseData(timestamp time.Time, datafile []byte) (dataset, error) {
	// marshal what data we can from the json
	parsed := make(map[string]dumpFormat)
	if err := json.Unmarshal(datafile, &parsed); err != nil {
		return []dumpFormat{}, fmt.Errorf("Error parsing bytes => %s", err.Error())
	}

	var (
		data   = make([]dumpFormat, len(parsed))
		i      int
		err    error
		exists bool
	)
	// add all fields needed to the JSON
	for id, d := range parsed {
		if d.GroupID, err = strconv.Atoi(id); err != nil {
			return []dumpFormat{}, fmt.Errorf("ERR: Failed to parse int, %s => %s", id, err.Error())
		}

		d.DumpTime = timestamp

		if d.ParentName, exists = parentNameLookup[d.ParentID]; !exists {
			log.Printf("ERROR: no parent name for %d exists in group: %d", d.ParentID, d.GroupID)
		}
		data[i] = d
		i++
	}

	return data, nil
}

// insert operates on a list of dumpFormat and inserts them to the provided Postgres
// database.
func (data dataset) insert(db *sql.DB) error {
	transaction, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Error starting PG txn => %s", err.Error())
	}

	// PG's COPY FROM used for fast mass insertions. Syntax is table followed by columns.
	// http://godoc.org/github.com/lib/pq#hdr-Bulk_imports
	stmt, err := transaction.Prepare(pq.CopyIn(
		"density_data", // table
		"dump_time",    // columns.....
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

	// Add all data from the set
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

	// execute the transaction
	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("Failed to execute bulk insert => %s", err.Error())
	}

	// commit the transaction if there's been no errors
	if err = transaction.Commit(); err != nil {
		log.Printf("ERROR: Failed to commit txn => %s", err.Error())
		if err = transaction.Rollback(); err != nil {
			log.Printf("ERROR: Failed to rollback txn => %s", err.Error())
		}
	}
	return nil
}
