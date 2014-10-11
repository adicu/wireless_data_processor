package main

import "testing"

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
		ID:          152,
		Name:        "Lerner 3",
		ClientCount: 70,
		ParentID:    84,
	},
	{
		ID:          131,
		Name:        "Butler Library 3",
		ClientCount: 328,
		ParentID:    103,
	},
	{
		ID:          155,
		Name:        "JJ's Place",
		ClientCount: 90,
		ParentID:    75,
	},
	{
		ID:          130,
		Name:        "Butler Library 2",
		ClientCount: 412,
		ParentID:    103,
	},
}

func TestParseData(t *testing.T) {
	data, err := parseData([]byte(testingData))
	if err != nil {
		t.Fatal(err)
	}

	// n^2 because n == 4....
	for _, d := range data {
		go func(t *testing.T) {
			for _, e := range expectedData {
				if d == e {
					return
				}
			}
			t.Errorf("No match in expected data for %#v\n", d)
		}(t)
	}
}
