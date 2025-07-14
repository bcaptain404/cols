# coLS

A flexible, programmable tool for wrangling CSV and tabular data from the command line.

Transform, filter, rename, merge, and process spreadsheet data with simple, scriptable configs.

---

## Example Usage

```sh
$ cols.py -c config.cong input1.csv input2.csv
```

---

## Features

Put these in your config. The order in which they are found is the order in which they are executed per-line. Files are processed fully one-at-a-time.

- Select columns by name or position for output (`use @N`, `use @"ColName"`, `use all`, `use @N-@M`)
- Rename columns by name or position (`rn @N "NewName"`, `rn @"Old" "New"`)
- Add new columns at any position (`add @N "NewCol"`)
- Set the value of an entire column (`set @N 42`, `set @"Name" "NULL"`)
- Replace text globally, in headers, or in selected columns/cells
- Move or swap columns by name or position
- Reorder, skip, truncate, limit, or tail input rows (`in head`, `in tail`, `in skip`, `in trunc`, `in max`)
- Handles multiple input files, printing header only once
- Comments and readable config files with bash-style `#` lines
- Robust error handling: ignores missing columns and warns on bad config
- Debug mode for step-by-step tracing

---

