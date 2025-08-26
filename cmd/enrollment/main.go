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

	query, err := os.ReadFile("queries/enrollment.sql")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(string(query), url)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	currentTime := time.Now()
	currentDate := currentTime.Format("2006-01")

	fileName := fmt.Sprintf("enrollment_%s.json", currentTime.Format("20060102150405"))
	if !isLocal {
		fileName = fmt.Sprintf("r2://scheduler-catalog/uvm/%s/enrollment/%s", currentDate, fileName)
	}

	_, err = db.Exec(fmt.Sprintf("copy enrollment to '%s' (array)", fileName))
	if err != nil {
		log.Fatal(fmt.Errorf("could not write to file: %w", err))
	}

	slog.Info("wrote enrollment", "file", fileName)

	defer db.Close()
}
