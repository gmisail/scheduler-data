# scheduler-data

A Go utility that scrapes historical catalog data for the University of Vermont.

## Tools
- Go 1.24+
- DuckDB

## Setup

Once you have the dependencies installed, run the following:

```bash
go run cmd/catalog/main.go
```

This will extract a number of files to the `upload/` directory:
- `terms.json` - all terms (202506, 202601, etc.) that are available in SIS
- `subjects_<term_id>.json` - subjects (ABIO, ASCI, CS, etc.)
- `catalog_<term_id>.json` - course catalog
