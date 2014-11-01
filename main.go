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
)

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
			getOrElse("PG_USER", "adicu"),
			getOrElse("PG_PASSWORD", ""),
			getOrElse("PG_DB", ""),
			getOrElse("PG_HOST", "localhost"),
			getOrElse("PG_PORT", "5432"),
			getOrElse("PG_SSL", "disable"),
		))
	if err != nil {
		log.Fatalf("Error connecting to Postgres => %s", err.Error())
	}
	return db
}

// handleFile processes new files
//
// The file is read into memory, parsed then inserted to the database.
func handleFile(filename string, db *sql.DB) {
	log.Printf("Processing, %s", filename)
	fileContents, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read in file => %s", err.Error())
	}

	tm, err := getDate(filename)
	if err != nil {
		log.Printf("Failed to parse date from filename, %s => %s", filename, err.Error())
		return
	}

	data, err := parseData(tm, fileContents)
	if err != nil {
		log.Fatal(err)
	}

	if err = dataset(data).insert(db); err != nil {
		log.Printf("Failed to insert data from, %s => %s", filename, err.Error())
		return
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
	db := dbConnect()
	defer db.Close()

	watchDir := flag.String("dir", ".", "directory to watch for new files")
	loadAll := flag.Bool("all", false, "load all dump file in the directory")
	keepWatching := flag.Bool("watch", true, "continue to watch for new files in the directory")

	flag.Parse()

	if *loadAll {
		log.Printf("Loading all files in directory, %s", *watchDir)

		files, err := ioutil.ReadDir(*watchDir)
		if err != nil {
			log.Fatalf("Failed to read in directory info => %s", err.Error())
		}

		for _, f := range files {
			handleFile(*watchDir+f.Name(), db)
		}
		updateViews(db)
	}

	if !*keepWatching {
		log.Println("Not watching for files as specified")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Failed to instantiate file watcher")
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Event:
				log.Println(event)
				if event.IsCreate() && filenameRegex.MatchString(event.Name) {
					handleFile(event.Name, db)
					updateViews(db)
				}
			case err := <-watcher.Error:
				log.Println(err)
			}
		}
	}()

	if err = watcher.Watch(*watchDir); err != nil {
		log.Fatalf("Failed to start watching directory, %s => %s", watchDir, err.Error())
	}

	<-done
}
