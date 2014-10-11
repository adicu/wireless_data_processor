package main

import "testing"

func TestFilenameRegex(t *testing.T) {
	if !filenameRegex.MatchString("2014-10-11-15-45.json") {
		t.Error("regex did not properly match")
	}
}
