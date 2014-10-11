package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/howeyc/fsnotify"
	_ "github.com/lib/pq"
)

// getOrElse checks the specified environment variable, returns the value if found, otherwise
// will return the default value provided. If there is no default then makes a fatal log.
func getOrElse(key, standard string) string {
	if val := os.Getenv(key); val != "" {
		return val
	} else if standard == "" {
		log.Fatalf("The environment variable, {}, must be set", key)
	}
	return standard
}

// dbConnect yanks db configurations from the environment variables and returns a postgres
// connection
func dbConnect() *sql.DB {
	db, err := sql.Open("postgres",
		fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s",
			getOrElse("PG_USER", "adicu"),
			getOrElse("PG_DB", ""),
			getOrElse("PG_PASSWORD", ""),
			getOrElse("PG_HOST", "localhost"),
			getOrElse("PG_PORT", "5432"),
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

	data, err := parseData(fileContents)
	if err != nil {
		log.Fatal(err)
	}

	if err = dataset(data).insert(db); err != nil {
		log.Printf("Failed to insert data from, %s =>", filename, err.Error())
	}
}

func main() {
	db := dbConnect()
	watchDir := *flag.String("dir", ".", "directory to watch for new files")

	if *flag.Bool("all", false, "load all dump file in the directory") {
		log.Printf("Loading all files in directory, %s", watchDir)

		files, err := ioutil.ReadDir(watchDir)
		if err != nil {
			log.Fatalf("Failed to read in directory info => %s", err.Error())
		}

		for _, f := range files {
			handleFile(f.Name(), db)
		}
	}

	if !*flag.Bool("watch", true, "continue to watch for new files in the directory") {
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
				if event.IsCreate() {
					handleFile(event.Name, db)
				}
			case err := <-watcher.Error:
				log.Println(err)
			}
		}
	}()

	if err = watcher.Watch(watchDir); err != nil {
		log.Fatalf("Failed to start watching directory, %s => %s", watchDir, err.Error())
	}

	<-done
}
