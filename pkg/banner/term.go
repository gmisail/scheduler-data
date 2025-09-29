package banner

import "github.com/gocolly/colly"

type Term struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

func ScrapeTerms() ([]Term, error) {
	url := "https://aisweb1.uvm.edu/pls/owa_prod/bwckctlg.p_display_courses?term_in=&one_subj=&sel_crse_strt=&sel_crse_end=&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr="

	terms := []Term{}

	c := colly.NewCollector()

	c.OnHTML("#term_input_id", func(e *colly.HTMLElement) {
		e.ForEach("option", func(_ int, o *colly.HTMLElement) {
			id := o.Attr("value")
			label := o.Text

			if id != "None" {
				terms = append(terms, Term{ID: id, Label: label})
			}
		})
	})

	c.Visit(url)

	return terms, nil
}
