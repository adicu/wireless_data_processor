package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
)

type dumpFormat struct {
	ID          int
	Name        string `json:"name"`
	ClientCount int    `json:"client_count"`
	ParentID    int    `json:"parent_id"`
}

func parseData(datafile []byte) ([]dumpFormat, error) {
	var parsed map[string]dumpFormat = make(map[string]dumpFormat)
	err := json.Unmarshal(datafile, &parsed)
	if err != nil {
		return []dumpFormat{}, fmt.Errorf("Error parsing file => %s", err.Error())
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
