package catalog

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/gocolly/colly"
)

var UNESCAPED_QUOTE_PATTERN = regexp.MustCompile(`([^,])"([^,])`)

type TermData struct {
	Label string
	Url   string
}

func ParseEnrollments(urls []string) (map[string]string, error) {
	enrollments := map[string]string{}

	for _, url := range urls {
		data, err := ParseEnrollment(url)
		if err != nil {
			return nil, fmt.Errorf("failed to parse enrollments: %w", err)
		}

		enrollments[data.Label] = data.Url

		slog.Info("found enrollment data file", slog.String("label", data.Label), slog.String("url", data.Url))
	}

	return enrollments, nil
}

func ParseEnrollment(url string) (*TermData, error) {
	data := TermData{}

	c := colly.NewCollector()

	c.OnHTML("h2", func(e *colly.HTMLElement) {
		if strings.Contains(e.Text, "Current Enrollment Counts") {
			data.Label = strings.TrimSpace(strings.TrimPrefix(e.Text, "Current Enrollment Counts "))
		}
	})

	c.OnHTML("a", func(e *colly.HTMLElement) {
		if strings.Contains(e.Text, "Click here to view in comma-delimited format") {
			data.Url = e.Attr("href")
		}
	})

	c.Visit(url)

	return &data, nil
}

func DownloadAndCleanData(url, file string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch csv data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch csv data: status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	for scanner.Scan() {
		cleaned := UNESCAPED_QUOTE_PATTERN.ReplaceAllString(scanner.Text(), "$1$2")

		if _, err := writer.WriteString(cleaned); err != nil {
			return nil, fmt.Errorf("failed to write cleaned data: %w", err)
		}

		if err := writer.WriteByte('\n'); err != nil {
			return nil, fmt.Errorf("failed to write cleaned data: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read csv data: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to write cleaned data: %w", err)
	}

	cleaned := buf.Bytes()

	if err := os.WriteFile(file, cleaned, 0o644); err != nil {
		return nil, fmt.Errorf("failed to write cleaned file: %w", err)
	}

	return cleaned, nil
}
