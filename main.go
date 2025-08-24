package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	keyId := os.Getenv("S3_KET_ID")
	secret := os.Getenv("S3_SECRET")
	accountId := os.Getenv("S3_ACCOUNT_ID")

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	url := "https://serval.uvm.edu/~rgweb/batch/curr_enroll_fall.txt"
	_, err = db.Exec(fmt.Sprintf(`
		install httpfs;
		load httpfs;

		create secret (
		    type r2,
			key_id '%s',
			secret '%s',
			account_id '%s'
		);

		create table semester_data as
		select * from read_csv (?);
	`, keyId, secret, accountId), url)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	query, err := os.ReadFile("query.sql")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(string(query), url)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	currentTime := time.Now().Format("2006-01-02 03:04:05")
	_, err = db.Exec(fmt.Sprintf("copy catalog to 'r2://scheduler-catalog/uvm_catalog_%s.json' (array)", currentTime))
	if err != nil {
		log.Fatal(fmt.Errorf("could not write to file: %w", err))
	}

	defer db.Close()
}
