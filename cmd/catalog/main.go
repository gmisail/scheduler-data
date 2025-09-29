package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/gmisail/scheduler-data/pkg/banner"
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

	terms, err := banner.ScrapeTerms()
	if err != nil {
		log.Fatal(err)
	}

	err = catalog.ExportTerms(terms)
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

	for label, url := range enrollments {
		for _, term := range terms {
			if term.Label == label {
				slog.Info("extracting catalog for term", slog.String("term", term.ID))
				catalog.ExtractCatalogForTerm(isLocal, term.ID, url)
			}
		}
	}
}
