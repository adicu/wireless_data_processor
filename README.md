
# Wireless Density Processor

[![Build Status](https://travis-ci.org/adicu/wireless_data_processor.svg)](https://travis-ci.org/adicu/wireless_data_processor)

Through a partnership with ESC & CUIT, ADI has been given access to snapshots of the number of people connected to WiFi for each floor in some library buildings.
We are building an API to provide this data ([project](github.com/adicu/density)).

This project is to watch for the file uploads from CUIT then parse the data before inserting it to the database.




## Testing

There are a handful of unit tests.
Travis-CI also allows us to run integration tests against Postgres.
There are 3 example dump files in `/test_data` that are used to test the actual executable.

See the Travis config for more details.




