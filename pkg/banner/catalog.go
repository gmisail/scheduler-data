package banner

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/gocolly/colly"
)

var CATALOG_TITLE_PATTERN = regexp.MustCompile(`^([A-Z]{2,4})\s+(\d{4})\s*-\s*(.+)$`)

type Course struct {
	Subject     string
	Number      string
	Title       string
	Description string
}

func ScrapeCoursesBySubject(db *sql.DB, semester, subject string) ([]Course, error) {
	c := colly.NewCollector()

	courses := []Course{}
	course := &Course{}

	c.OnHTML("table.datadisplaytable", func(e *colly.HTMLElement) {
		e.ForEach("td", func(id int, d *colly.HTMLElement) {
			if d.DOM.HasClass("nttitle") {
				matches := CATALOG_TITLE_PATTERN.FindStringSubmatch(d.Text)

				if len(matches) == 4 {
					course = &Course{
						Subject:     matches[1],
						Number:      matches[2],
						Title:       matches[3],
						Description: "",
					}
				} else {
					course = nil
				}
			} else if d.DOM.HasClass("ntdefault") {
				if course != nil {
					desc := ""

					if first := d.DOM.Contents().First(); first != nil {
						trimmedDesc := strings.TrimSpace(first.Text())
						desc = strings.Join(strings.Split(trimmedDesc, "\n"), " ")
					}

					course.Description = desc

					courses = append(courses, *course)
				}
			}
		})
	})

	err := c.Visit(
		fmt.Sprintf(
			"https://aisweb1.uvm.edu/pls/owa_prod/bwckctlg.p_display_courses?term_in=%s&one_subj=%s&sel_crse_strt=&sel_crse_end=&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr=",
			semester,
			subject,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to visit url: %w", err)
	}

	return courses, nil
}
