package main

import (
	"testing"
	"time"
)

var (
	testFilename = "2014-10-11-15-45.json"
	expectedTime = time.Date(2014, time.October, 11, 15, 45, 0, 0, &time.Location{})
)

func TestFilenameRegex(t *testing.T) {
	if !filenameRegex.MatchString(testFilename) {
		t.Error("regex did not properly match")
	}
}

func TestPullDate(t *testing.T) {
	tm, err := getDate(testFilename)
	if err != nil || tm.String() != expectedTime.String() {
		t.Errorf("Failed to properly parse date, found %s, expected %s", tm, expectedTime)
	}
}
