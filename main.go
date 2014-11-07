package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/howeyc/fsnotify"
	_ "github.com/lib/pq"
)

var (
	filenameRegex     = regexp.MustCompile(`(\d{4}(-\d{2}){4})\.json$`)
	datetimeRegex     = regexp.MustCompile(`([\d-]*)`)
	datetimeFormat    = "2006-01-02-15-04"
	materializedViews = []string{
		"hour_window",
		"day_window",
		"week_window",
		"month_window",
	}
	maxHandlesNum = 100
	PG_USER       = getOrElse("PG_USER", "adicu")
	PG_PASSWORD   = getOrElse("PG_PASSWORD", "")
	PG_DB         = getOrElse("PG_DB", "")
	PG_HOST       = getOrElse("PG_HOST", "localhost")
	PG_PORT       = getOrElse("PG_PORT", "5432")
	PG_SSL        = getOrElse("PG_SSL", "disable")
)

// getDate parses a filepath to get a date from the filename given the regex
// declared in `filenameRegex`.
func getDate(s string) (time.Time, error) {
	return time.Parse(datetimeFormat, datetimeRegex.FindString(path.Base(s)))
}

// getOrElse checks the specified environment variable, returns the value if found, otherwise
// will return the default value provided. If there is no default then makes a fatal log.
func getOrElse(key, standard string) string {
	if val := os.Getenv(key); val != "" {
		return val
	} else if standard == "" {
		log.Fatalf("The environment variable, %s, must be set", key)
	}
	return standard
}

// dbConnect yanks db configurations from the environment variables and returns a postgres
// connection
func dbConnect() *sql.DB {
	db, err := sql.Open("postgres",
		fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
			PG_USER,
			PG_PASSWORD,
			PG_DB,
			PG_HOST,
			PG_PORT,
			PG_SSL,
		))
	if err != nil {
		log.Fatalf("Error connecting to Postgres => %s", err.Error())
	}
	log.Printf("Databse connection made to %s", PG_DB)
	return db
}

// handleFile processes new files
//
// The file is read into memory, parsed then inserted to the database.
func handleFile(filename string, db *sql.DB) {
	log.Printf("Processing, %s", filename)
	fileContents, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("Failed to read in file, %s => %s", filename, err.Error())
		return
	}

	tm, err := getDate(filename)
	if err != nil {
		log.Printf("Failed to parse date from file, %s, ignored.", filename)
		return
	}

	data, err := parseData(tm, fileContents)
	if err != nil {
		log.Printf("Failed to parse data from %s => %s", filename, err.Error())
		return
	}

	if err = dataset(data).insert(db); err != nil {
		log.Printf("Failed to insert data from, %s => %s", filename, err.Error())
	}
}

// Update the materialized views listed in `materializedViews`
func updateViews(db *sql.DB) {
	txn, err := db.Begin()
	if err != nil {
		log.Printf("ERR: failed to start pq txn for materialized view updates => %s", err.Error())
		return
	}

	for _, view := range materializedViews {
		if _, err = txn.Exec(fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", view)); err != nil {
			log.Printf("Failed to update materialized view, %s => %s", view, err.Error())
		}
	}

	if nil == txn.Commit() {
		log.Println("materialized views updated")
	}
}

func main() {
	var (
		watchDir     = flag.String("dir", ".", "directory to watch for new files")
		loadAll      = flag.Bool("all", false, "load all dump file in the directory")
		keepWatching = flag.Bool("watch", true, "continue to watch for new files in the directory")
	)
	flag.Parse()

	// if all the files currently in the directory should be loaded
	if *loadAll {
		db := dbConnect()

		log.Printf("Loading all files in directory, %s", *watchDir)
		files, err := ioutil.ReadDir(*watchDir)
		if err != nil {
			log.Fatalf("Failed to read in directory info => %s", err.Error())
		}

		// handle every data file
		for _, f := range files {
			handleFile(path.Join(*watchDir, f.Name()), db)
		}

		updateViews(db) // refresh the materialized views afterwards
		db.Close()
	}

	// exits if flag turned on
	if !*keepWatching {
		log.Println("Not watching for files as specified")
		return
	}

	// start watching for new files
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Failed to instantiate file watcher")
	}
	defer watcher.Close()

	// start the file system watcher
	if err = watcher.Watch(*watchDir); err != nil {
		log.Fatalf("Failed to start watching directory, %s => %s", *watchDir, err.Error())
	}

	// wait for any new files to be added, then process them
	for {
		select {
		case event := <-watcher.Event:
			db := dbConnect()
			if event.IsCreate() && filenameRegex.MatchString(event.Name) {
				time.Sleep(time.Duration(2 * time.Second))
				handleFile(event.Name, db)
				updateViews(db)
			}
			db.Close()
		case err := <-watcher.Error:
			log.Println(err)
		}
	}
}
