package catalog

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gmisail/scheduler-data/pkg/banner"
	"github.com/gmisail/scheduler-data/pkg/util"
)

func GetLatestTerm() (*banner.Term, error) {
	terms, err := banner.ScrapeTerms()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest term: %w", err)
	}

	if len(terms) == 0 {
		return nil, fmt.Errorf("expected there to be multiple terms, found none: %w", err)
	}

	return &terms[0], nil
}

func ExtractCatalogForTerm(isLocal bool, term, url string) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	slog.Info("load secrets")
	err = LoadSecrets(db)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to load secrets: %w", err))
	}

	slog.Info("setup schema")
	util.RunQueryFromFile(db, "queries/schema.sql")
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from catalog data: %w", err))
	}

	slog.Info("scrape subjects")
	subjects, err := banner.ScrapeSubjects(term)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to scrape subjects: %w", err))
	}

	slog.Info("scrape courses for subjects")
	for _, subject := range subjects {
		courses, err := banner.ScrapeCoursesBySubject(db, term, subject.ID)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to scrape courses by subject: %w", err))
		}

		for _, course := range courses {
			_, err := db.Exec(
				"INSERT INTO course_desc VALUES (?, ?, ?);",
				course.Subject,
				course.Number,
				course.Description,
			)
			if err != nil {
				log.Fatal(fmt.Errorf("failed to insert course description: %w", err))
			}
		}
	}

	file := fmt.Sprintf("%s.csv", term)
	_, err = DownloadAndCleanData(url, file)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to download and clean data: %w", err))
	}

	slog.Info("loading enrollment data")
	_, err = db.Exec(`
		CREATE TABLE semester_data AS
		SELECT * FROM read_csv(?);
	`, file)
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from semester data: %w", err))
	}

	err = os.Remove(file)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to delete enrollment data file: %w", err))
	}

	util.RunQueryFromFile(db, "queries/catalog.sql")
	if err != nil {
		log.Fatal(fmt.Errorf("could not pull data from catalog data: %w", err))
	}

	err = ExportCatalog(db, isLocal, term)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to export catalog: %w", err))
	}
}

func ExportCatalog(db *sql.DB, isLocal bool, term string) error {
	catalogFile := fmt.Sprintf("r2://scheduler-catalog/uvm/%s/catalog.json", term)

	if isLocal {
		catalogFile = "catalog.json"
	}

	err := util.ExportTableAs(db, "catalog", catalogFile)
	if err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	slog.Info("wrote catalog", "file", catalogFile)

	return nil
}
