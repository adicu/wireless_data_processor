
# Wireless Density Processor

[![Build Status](https://travis-ci.org/adicu/wireless_data_processor.svg)](https://travis-ci.org/adicu/wireless_data_processor)

Through a partnership with ESC & CUIT, ADI has been given access to snapshots of the number of people connected to WiFi for each floor in some library buildings.
We are building an API to provide this data ([project](github.com/adicu/density)).

This project is to watch for the file uploads from CUIT then parse the data before inserting it to the database.



## Deployment

```
Usage of ./wireless_data_processor:
  -all=false: load all dump file in the directory
  -dir=".": directory to watch for new files
  -watch=true: continue to watch for new files in the directory
```

If deploying for the first time, the `all` flag should be used to load every single file in the directory.
Otherwise only the `watch` command will be needed.
This will watch for new files and add them as they appear.




## Testing


### Unit Tests

There are several straightforward unit tests for parsing data.


### Integration Tests

Travis-CI also allows us to run integration tests against Postgres.
There are 3 example dump files in `/test_data` that are used to test the actual executable.

Travis creates the database using `schema.sql`.
Then the executable is run using the option to load all files in the directory.




