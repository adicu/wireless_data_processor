package main

import (
	"testing"
	"time"
)

var (
	testFilename     = "2014-10-11-15-45.json"
	testFilename2    = "test_data/2014-10-11-15-45.json"
	testFilenameFail = "201-10-11-1-45.json"
	tz, _            = time.LoadLocation("America/New_York")
	expectedTime     = time.Date(2014, time.October, 11, 15, 45, 0, 0, tz)
)

func TestFilenameRegex(t *testing.T) {
	if !filenameRegex.MatchString(testFilename) {
		t.Error("regex did not properly match")
	}

	if !filenameRegex.MatchString(testFilename2) {
		t.Error("regex did not properly match")
	}

	if filenameRegex.MatchString(testFilenameFail) {
		t.Error("regex should not have matched")
	}
}

func TestPullDate(t *testing.T) {
	tm, err := getDate(testFilename)
	if err != nil || tm.String() != expectedTime.String() {
		t.Errorf("Failed to properly parse date, found %s, expected %s", tm, expectedTime)
	}

	tm, err = getDate(testFilename2)
	if err != nil {
		t.Errorf("Failed to properly parse date")
	}

	_, err = getDate(testFilenameFail)
	if err == nil {
		t.Error("Improper date parsed without failure")
	}
}
