The application provides REPL with SQL-like language to query CSV given.

## Usage

```csvsh --help
Usage: cmd [OPTIONS]

Options:
  -c, --cmd TEXT  SQL statement to run
  -h, --help      Show this message and exit
```

## Examples

```


```

## Features

- [x] `SELECT` query with `GROUP BY`, `ORDER BY` and `LIMIT` statements.
- [x] Aggregates: `COUNT`, `MIN`, `MAX`, `SUM`
- [x] `DESCRIBE` query for `SELECTps and CSV data.
- [x] Data types: string, double and long.
- [x] `INDEXES` for any datatype.
- [x] Full-featured REPL (history, autocomplete, etc..) and one-shot execution modes.

## Build

`./gradlew build`

