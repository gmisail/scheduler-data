package main

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/gocolly/colly"
	"github.com/joho/godotenv"

	_ "github.com/marcboeker/go-duckdb"
)

type Course struct {
	Title       string `selector:".courseblocktitle"`
	Description string `selector:".courseblockdesc"`
}

func normalizeSpaces(word string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, word)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
		return
	}

	keyId := os.Getenv("S3_KEY_ID")
	secret := os.Getenv("S3_SECRET")
	accountId := os.Getenv("S3_ACCOUNT_ID")

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`
		install httpfs;
		load httpfs;

		create secret (
		    type r2,
			key_id '%s',
			secret '%s',
			account_id '%s'
		);

		create table department (
			id text primary key not null,
			name text not null,
		);

		create table course_desc (
			subject text not null,
			number text not null,
			description text not null
		);
	`, keyId, secret, accountId))
	if err != nil {
		log.Fatal(err)
	}

	c := colly.NewCollector()

	c.OnHTML("div#atozindex ul li a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		nextUrl := e.Request.AbsoluteURL(link)

		err := c.Visit(nextUrl)
		if err != nil {
			log.Fatal(err)
		}

		re := regexp.MustCompile(`(.*)\s+\((.*)\)`)
		match := re.FindStringSubmatch(e.Text)

		if len(match) > 2 {
			label := match[1]
			code := match[2]

			db.Exec("INSERT INTO department (id, name) VALUES (?, ?)", code, label)
			if err != nil {
				log.Fatalf("Error inserting department %s (%s): %v", label, code, err)
			}
		}
	})

	c.OnHTML(".courses", func(e *colly.HTMLElement) {
		re := regexp.MustCompile(`([A-Z]+)\s+(\d{4})`)

		e.ForEach(".courseblock", func(_ int, block *colly.HTMLElement) {
			c := Course{}

			err := block.Unmarshal(&c)
			if err != nil {
				log.Fatal(err)
			}

			c.Title = normalizeSpaces(c.Title)
			c.Description = strings.TrimSpace(normalizeSpaces(c.Description))

			match := re.FindStringSubmatch(c.Title)
			subject := match[1]
			number := match[2]

			_, err = db.Exec(`
				INSERT INTO course_desc (subject, number, description)
				VALUES (?, ?, ?);
			`, subject, number, c.Description)
			if err != nil {
				log.Fatal(err)
			}
		})
	})

	err = c.Visit("https://catalogue.uvm.edu/undergraduate/courses/courselist/")
	if err != nil {
		log.Fatalf("Error visiting URL: %v", err)
		return
	}
	slog.Info("done scraping from undergraduate course list")

	err = c.Visit("https://catalogue.uvm.edu/graduate/courses/courselist/")
	if err != nil {
		log.Fatalf("Error visiting URL: %v", err)
		return
	}
	slog.Info("done scraping from graduate course list")

	currentTime := time.Now().Format("2006-01")
	departmentFile := fmt.Sprintf("r2://scheduler-catalog/uvm/%s/departments.json", currentTime)
	_, err = db.Exec(fmt.Sprintf(`
		copy department to '%s' (array);
		copy course_desc to 'course_desc.csv';
	`, departmentFile))
	if err != nil {
		log.Fatal(fmt.Errorf("could not write to file: %w", err))
	}
}
