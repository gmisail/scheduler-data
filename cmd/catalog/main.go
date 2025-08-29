package main

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn("could not load .env", "err", err)
	}

	isLocal := os.Getenv("ENV") == "local"

	keyId := os.Getenv("S3_KEY_ID")
	secret := os.Getenv("S3_SECRET")
	accountId := os.Getenv("S3_ACCOUNT_ID")

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("create table course_desc as select * from read_csv('course_desc.csv');")
	if err != nil {
		log.Fatal(fmt.Errorf("could not load course descriptions: %w", err))
	}

	secrets := fmt.Sprintf(`
		create secret bucket (
		    type r2,
			key_id '%s',
			secret '%s',
			account_id '%s'
		);
	`, keyId, secret, accountId)
	_, err = db.Exec(secrets)
	if err != nil {
		log.Fatal(fmt.Errorf("could load secrets: %w", err))
	}

	url := "https://serval.uvm.edu/~rgweb/batch/curr_enroll_fall.txt"
	_, err = db.Exec(`
		install httpfs;
		load httpfs;

		create table semester_data as
		select * from read_csv (?);
	`, url)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	query, err := os.ReadFile("queries/catalog.sql")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(string(query), url)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	currentTime := time.Now()
	currentDate := currentTime.Format("2006-01")
	catalogFile := fmt.Sprintf("r2://scheduler-catalog/uvm/%s/catalog.json", currentDate)

	if isLocal {
		catalogFile = "catalog.json"
	}

	_, err = db.Exec(fmt.Sprintf("copy catalog to '%s' (array)", catalogFile))
	if err != nil {
		log.Fatal(fmt.Errorf("could not write to file: %w", err))
	}

	slog.Info("wrote catalog", "file", catalogFile)

	defer db.Close()
}
