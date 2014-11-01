package main

import (
	"testing"
	"time"
)

var testingData string = `{
  "152" : {
    "name" : "Lerner 3",
    "client_count" : 70,
    "parent_id" : 84
  },
  "131" : {
    "name" : "Butler Library 3",
    "client_count" : 328,
    "parent_id" : 103
  },
  "155" : {
    "name" : "JJ's Place",
    "client_count" : 90,
    "parent_id" : 75
  },
  "130" : {
    "name" : "Butler Library 2",
    "client_count" : 412,
    "parent_id" : 103
  }
}`

var expectedData []dumpFormat = []dumpFormat{
	{
		GroupID:     152,
		GroupName:   "Lerner 3",
		ClientCount: 70,
		ParentID:    84,
		ParentName:  "Lerner",
	},
	{
		GroupID:     131,
		GroupName:   "Butler Library 3",
		ClientCount: 328,
		ParentID:    103,
		ParentName:  "Butler",
	},
	{
		GroupID:     155,
		GroupName:   "JJ's Place",
		ClientCount: 90,
		ParentID:    75,
		ParentName:  "John Jay",
	},
	{
		GroupID:     130,
		GroupName:   "Butler Library 2",
		ClientCount: 412,
		ParentID:    103,
		ParentName:  "Butler",
	},
}

func TestParseData(t *testing.T) {
	data, err := parseData(time.Time{}, []byte(testingData))
	if err != nil {
		t.Fatal(err)
	}

	// n^2 because n == 4....
	for _, d := range data {
		go func(d dumpFormat, t *testing.T) {
			for _, e := range expectedData {
				if d == e {
					return
				}
			}
			t.Errorf("No match in expected data for %#v\n", d)
		}(d, t)
	}
}
