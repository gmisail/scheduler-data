package catalog

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gmisail/scheduler-data/pkg/banner"
	"github.com/gmisail/scheduler-data/pkg/util"
)

func GetLatestTerm(terms []banner.Term) (*banner.Term, error) {
	if len(terms) == 0 {
		return nil, fmt.Errorf("expected there to be multiple terms, found none")
	}

	return &terms[0], nil
}

func ExtractCatalogForTerm(isLocal bool, term, url string) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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

	err = ExportSubjects(term, subjects)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to export subjects: %w", err))
	}
}

func ExportCatalog(db *sql.DB, isLocal bool, term string) error {
	catalogFile := fmt.Sprintf("upload/catalog_%s.json", term)

	err := util.ExportTableAs(db, "catalog", catalogFile)
	if err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	slog.Info("wrote catalog", "file", catalogFile)

	return nil
}

func ExportTerms(terms []banner.Term) error {
	file, err := os.Create("upload/terms.json")
	if err != nil {
		return fmt.Errorf("failed to create terms file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(terms); err != nil {
		return fmt.Errorf("failed to write terms: %w", err)
	}

	slog.Info("wrote terms to file", "file", "terms.json")

	return nil
}

func ExportSubjects(term string, subjects []banner.Subject) error {
	file, err := os.Create(fmt.Sprintf("upload/subjects_%s.json", term))
	if err != nil {
		return fmt.Errorf("failed to create subjects file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(subjects); err != nil {
		return fmt.Errorf("failed to write subjects: %w", err)
	}

	slog.Info("wrote subjects to file", "file", "subjects.json")

	return nil
}
