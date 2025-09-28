package util

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

func RunQueryFromFile(db *sql.DB, file string) {
	query, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(string(query))
	if err != nil {
		log.Fatal(fmt.Errorf("could not run query from file: %w", err))
	}
}

func ExportTableAs(db *sql.DB, table, file string) error {
	_, err := db.Exec(fmt.Sprintf("copy %s to '%s' (array)", table, file))
	if err != nil {
		return fmt.Errorf("failed to copy table to file: %w", err)
	}

	return nil
}
