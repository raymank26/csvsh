The application provides REPL with SQL-like language to query CSV located in filesystem.

## Installation

MacOS
```
brew install raymank26/tools/csvsh
```

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

3. More complicated query with `GROUP BY`, `ORDER BY`, `LIMIT`

```
csvsh>> SELECT city, COUNT(*) FROM '/test/input2.csv' GROUP BY city WHERE city <> '' ORDER BY city LIMIT 5

╔══════════════╤══════════╗
║ city         │ count(*) ║
╠══════════════╪══════════╣
║ Acton        │ 3        ║
╟──────────────┼──────────╢
║ Agoura Hills │ 2        ║
╟──────────────┼──────────╢
║ Alameda      │ 1        ║
╟──────────────┼──────────╢
║ Albuquerque  │ 3        ║
╟──────────────┼──────────╢
║ Aliso Viejo  │ 2        ║
╚══════════════╧══════════╝
```

## Features

- [x] `SELECT` query with `GROUP BY`, `ORDER BY` and `LIMIT` statements.
- [x] Aggregates: `COUNT`, `MIN`, `MAX`, `SUM`.
- [x] `DESCRIBE` query for `SELECT` and CSV data.
- [x] Data types: string, double and long.
- [x] `INDEXES` for any datatype above.
- [x] Full-featured REPL (history, autocomplete, etc..) and one-shot execution mode.

## Contributing

1. Fork the repo.
2. Run build via `./gradlew build`
3. Add a feature/fix a bug, add tests.
4. Make a pull request.

