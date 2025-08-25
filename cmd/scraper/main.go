package main

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"strings"
	"unicode"

	"github.com/gocolly/colly"

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

func pullDepartments() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE department (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
		);

		CREATE TABLE course_desc (
			subject TEXT NOT NULL,
			number TEXT NOT NULL,
			description TEXT NOT NULL
		);
	`)
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

	_, err = db.Exec(`
		copy department to 'departments.csv';
		copy course_desc to 'course_desc.csv';
	`)
	if err != nil {
		log.Fatal(fmt.Errorf("could not write to file: %w", err))
	}
}

func main() {
	pullDepartments()
}
