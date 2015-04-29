package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func init() {
	os.Setenv("PG_USER", "adicu")
	os.Setenv("PG_PASSWORD", "adicu")
	os.Setenv("PG_DB", "density")
	os.Setenv("PG_HOST", "localhost")
	os.Setenv("PG_PORT", "5432")
	os.Setenv("PG_SSL", "disable")
}

// struct w/ regular encoding
var data1 = `{
    "name" : "Lerner 3",
    "client_count" : 70,
    "parent_id" : 84
}`

// struct w/ string-encoded numbers
var data2 = `{
    "name" : "Lerner 3",
    "client_count" : "70",
    "parent_id" : "84"
}`

// TestUnmarshalData tests that we now properly unmarshal both forms
// of the data.
func TestUnmarshalData(t *testing.T) {
	var d dumpFormat

	err := json.Unmarshal([]byte(data1), &d)
	if err != nil {
		t.Fatalf("Failed to unmarshal data with integers => {%s}", err)
	}

	err = json.Unmarshal([]byte(data2), &d)
	if err != nil {
		t.Fatalf("Failed to unmarshal data with strings => {%s}", err)
	}
}

var testingData1 = `{
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

var testingData2 = `{
  "152" : {
    "name" : "Lerner 3",
    "client_count" : "70",
    "parent_id" : "84"
  },
  "131" : {
    "name" : "Butler Library 3",
    "client_count" : "328",
    "parent_id" : "103"
  },
  "155" : {
    "name" : "JJ's Place",
    "client_count" : "90",
    "parent_id" : "75"
  },
  "130" : {
    "name" : "Butler Library 2",
    "client_count" : "412",
    "parent_id" : "103"
  }
}`

var expectedData = []dumpFormat{
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

// TestParseData parses the raw data in `testingData` and confirms that it
// correctly configures all data fields.
func TestParseData(t *testing.T) {
	for _, dataset := range []string{testingData1, testingData2} {
		data, err := parseData(time.Time{}, []byte(dataset))
		if err != nil {
			t.Fatal(err)
		}

		// make sure that the parsed data is in the expected data
		expected := func(d dumpFormat, t *testing.T) {
			for _, e := range expectedData {
				if d == e {
					return
				}
			}
			t.Errorf("No match in expected data for %#v\n", d)
		}

		// n^2 because n == 4....
		for _, d := range data {
			expected(d, t)
		}
	}
}
