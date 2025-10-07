package banner

import (
	"fmt"

	"github.com/gocolly/colly"
)

type Subject struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

func ScrapeSubjects(semester string) ([]Subject, error) {
	url := fmt.Sprintf(
		"https://aisweb1.uvm.edu/pls/owa_prod/bwckctlg.p_display_courses?term_in=%s&one_subj=&sel_crse_strt=&sel_crse_end=&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr=",
		semester,
	)

	subjects := []Subject{}

	c := colly.NewCollector()

	c.OnHTML("#subj_id", func(e *colly.HTMLElement) {
		e.ForEach("option", func(_ int, o *colly.HTMLElement) {
			id := o.Attr("value")
			label := o.Text
			subjects = append(subjects, Subject{ID: id, Label: label})
		})
	})

	c.Visit(url)

	return subjects, nil
}
