The application provides REPL with SQL-like language to query CSV located in filesystem.

## Usage

```csvsh --help
Usage: cmd [OPTIONS]

Options:
  -c, --cmd CMD  SQL statement to run
  -v, --version  Show version
  -h, --help     Show this message and exit
```

## Examples

1. Get count of all rows in CSV:
```
csvsh>> SELECT COUNT(*) FROM '/path/to/csv'

╔══════════╗
║ count(*) ║
╠══════════╣
║ 7        ║
╚══════════╝
```

2. `WHERE` conditions evaluation:
```
csvsh>> SELECT * FROM '/test/input.csv' WHERE a IN ('bazz', 'баз') AND c IN (-1.0) AND b NOT IN (2)

╔══════╤══════╤══════╗
║ a    │ b    │ c    ║
╠══════╪══════╪══════╣
║ bazz │ null │ -1.0 ║
╚══════╧══════╧══════╝
```

## Features

- [x] `SELECT` query with `GROUP BY`, `ORDER BY` and `LIMIT` statements.
- [x] Aggregates: `COUNT`, `MIN`, `MAX`, `SUM`
- [x] `DESCRIBE` query for `SELECT` and CSV data.
- [x] Data types: string, double and long.
- [x] `INDEXES` for any datatype above.
- [x] Full-featured REPL (history, autocomplete, etc..) and one-shot execution mode.

## Build

`./gradlew build`

