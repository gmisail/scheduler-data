package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gmisail/scheduler-data/pkg/catalog"
	"github.com/joho/godotenv"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn("could not load .env", "err", err)
	}

	isLocal := os.Getenv("ENV") == "local"

	slog.Info("looking for latest term")
	term, err := catalog.GetLatestTerm()
	if err != nil {
		log.Fatal(err)
	}

	enrollments, err := catalog.ParseEnrollments([]string{
		"https://serval.uvm.edu/~rgweb/batch/curr_enroll_fall.html",
		"https://serval.uvm.edu/~rgweb/batch/curr_enroll_spring.html",
		"https://serval.uvm.edu/~rgweb/batch/curr_enroll_summer.html",
	})
	if err != nil {
		log.Fatal(err)
	}

	if currentUrl, ok := enrollments[term.Label]; ok {
		slog.Info("found enrollment data for latest term, extracting catalog", slog.String("term", term.ID))

		catalog.ExtractCatalogForTerm(isLocal, term.ID, currentUrl)
	} else {
		log.Fatal(fmt.Errorf("failed to find enrollment data for the latest term: %s (%s)", term.Label, term.ID))
	}
}
