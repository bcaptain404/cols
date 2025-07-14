#!/usr/bin/env python3

import csv
import argparse
import sys
import re

def debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr)

def parse_config(cfg_path):
    ops = []
    specs = {}
    with open(cfg_path) as f:
        for lineno, line in enumerate(f, 1):
            orig_line = line.rstrip('\n')
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("in "):
                keyval = line[3:].split()
                if keyval:
                    specs[keyval[0]] = keyval[1] if len(keyval) > 1 else True
                debug(f"Read spec (line {lineno}): {orig_line}")
                continue
            ops.append(line)
            debug(f"Read op (line {lineno}): {orig_line}")
    return specs, ops

def parse_col(ref, header):
    if not ref.startswith('@'):
        return None
    ref = ref[1:]
    if ref.startswith('"') and ref.endswith('"'):
        name = ref[1:-1]
        if name in header:
            return header.index(name)
        else:
            return None
    try:
        idx = int(ref)
        if idx < len(header):
            return idx
        else:
            return None
    except ValueError:
        return None

def do_replace(val, find, repl):
    return val.replace(find, repl)

def do_swap(lst, a, b):
    lst[a], lst[b] = lst[b], lst[a]

def do_move(lst, a, b):
    col = lst.pop(a)
    lst.insert(b, col)

def streaming_tail(src, n):
    from collections import deque
    dq = deque(maxlen=n)
    for row in src:
        dq.append(row)
    return list(dq)

def streaming_trunc(src, n):
    buf = []
    for row in src:
        buf.append(row)
    return buf[:-n] if n > 0 else buf

def process_csv(input_path, specs, ops, global_delim=',', global_strdelim='"'):
    # Output range/col tracking
    operated_cols = []
    last_use_all = False
    last_use_range = None  # tuple (start_idx, end_idx)
    last_use_single = []   # list of indices
    with open(input_path, newline='') as f:
        delim = specs.get('delim', global_delim)
        strdelim = specs.get('str', global_strdelim)
        reader = csv.reader(f, delimiter=delim, quotechar=strdelim)

        header = next(reader)
        col_names = list(header)
        rows = [list(row) for row in reader]

        debug(f"Input header: {col_names} (len={len(col_names)})")

        # Handle row-specs
        if 'skip' in specs:
            debug(f"Applying spec: skip {specs['skip']} rows")
            rows = rows[int(specs['skip']):]
        if 'head' in specs:
            debug(f"Applying spec: head {specs['head']} rows")
            rows = rows[:int(specs['head'])]
        if 'tail' in specs:
            debug(f"Applying spec: tail {specs['tail']} rows")
            rows = streaming_tail(rows, int(specs['tail']))
        if 'trunc' in specs:
            debug(f"Applying spec: trunc {specs['trunc']} rows")
            rows = streaming_trunc(rows, int(specs['trunc']))
        if 'max' in specs:
            debug(f"Applying spec: max {specs['max']} rows")
            rows = rows[:int(specs['max'])]

        for op in ops:
            tokens = re.findall(r'@[0-9]+|@\"[^\"]+\"|\".+?\"|\S+', op)
            cmd = tokens[0]
            args = tokens[1:]
            debug(f"Processing op: {op}")
            debug(f"Current col_names before '{op}': {col_names}")

            if cmd == "use":
                # --- use all ---
                if len(args) == 1 and args[0] == "all":
                    debug(f"Processing 'use all': will use all columns after mutations")
                    last_use_all = True
                    last_use_range = None
                    last_use_single = []
                    continue
                # --- use @N-@M ---
                if len(args) == 1 and re.match(r'^@(\d+)-@(\d+)$', args[0]):
                    m = re.match(r'^@(\d+)-@(\d+)$', args[0])
                    start, end = int(m.group(1)), int(m.group(2))
                    debug(f"Processing 'use @{start}-@{end}': will use columns {start} through {end} after mutations")
                    last_use_all = False
                    last_use_range = (start, end)
                    last_use_single = []
                    continue
                # --- use @N or @"Name" ---
                idx = parse_col(args[0], col_names)
                if idx is None or idx >= len(col_names):
                    debug(f"Skipping op 'use' for missing column {args[0]}")
                    continue
                debug(f"Adding column {idx} to output columns (use)")
                last_use_all = False
                last_use_range = None
                last_use_single.append(idx)
            # --- (rest of your ops unchanged, e.g. rn/add/set/replace/etc) ---
            elif cmd == "rn":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    debug(f"Skipping op 'rn' for missing column {args[0]}")
                    continue
                newname = args[1].strip('"')
                debug(f"Renaming column {idx} to {newname}")
                col_names[idx] = newname
            elif cmd == "add":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    debug(f"Skipping op 'add' for missing column {args[0]}")
                    continue
                col_names.insert(idx, args[1].strip('"'))
                for row in rows:
                    row.insert(idx, "")
            elif cmd == "set":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    debug(f"Skipping op 'set' for missing column {args[0]}")
                    continue
                val = args[1].strip('"')
                debug(f"Setting all values in col {idx} to {val}")
                for row in rows:
                    row[idx] = val
            elif cmd == "replace_all":
                find, repl = args[0].strip('"'), args[1].strip('"')
                debug(f"Replacing all '{find}' with '{repl}' globally")
                col_names = [do_replace(x, find, repl) for x in col_names]
                for row in rows:
                    for j, cell in enumerate(row):
                        row[j] = do_replace(cell, find, repl)
            elif cmd == "replace_head":
                find, repl = args[0].strip('"'), args[1].strip('"')
                debug(f"Replacing '{find}' with '{repl}' in headers only")
                col_names = [do_replace(x, find, repl) for x in col_names]
            elif cmd == "replace_cell":
                find, repl = args[0].strip('"'), args[1].strip('"')
                debug(f"Replacing '{find}' with '{repl}' in all cell data")
                for row in rows:
                    for j, cell in enumerate(row):
                        row[j] = do_replace(cell, find, repl)
            elif cmd == "replace":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    debug(f"Skipping op 'replace' for missing column {args[0]}")
                    continue
                find, repl = args[1].strip('"'), args[2].strip('"')
                debug(f"Replacing '{find}' with '{repl}' in column {idx}")
                for row in rows:
                    row[idx] = do_replace(row[idx], find, repl)
            elif cmd == "move":
                a = parse_col(args[0], col_names)
                b = parse_col(args[1], col_names)
                if a is None or b is None:
                    debug(f"Skipping op 'move' for missing columns {args[0]}, {args[1]}")
                    continue
                debug(f"Moving column {a} to {b}")
                do_move(col_names, a, b)
                for row in rows:
                    do_move(row, a, b)
            elif cmd == "swap":
                a = parse_col(args[0], col_names)
                b = parse_col(args[1], col_names)
                if a is None or b is None:
                    debug(f"Skipping op 'swap' for missing columns {args[0]}, {args[1]}")
                    continue
                debug(f"Swapping columns {a} and {b}")
                do_swap(col_names, a, b)
                for row in rows:
                    do_swap(row, a, b)
            elif cmd == "in" and args[0] == "col" and args[2] == "str":
                idx = int(args[1])
                if idx is None:
                    debug(f"Skipping op 'in col ... str' for missing column {args[1]}")
                    continue
                debug(f"Set string delimiter for col {idx}: {args[3]}")
            else:
                if cmd not in ("in",):
                    print(f"Warning: Unrecognized op '{op}'", file=sys.stderr)

        # Determine output columns
        if last_use_all:
            out_col_idxs = list(range(len(col_names)))
        elif last_use_range is not None:
            start, end = last_use_range
            out_col_idxs = [i for i in range(start, end+1) if i < len(col_names)]
        elif last_use_single:
            # Preserve order, remove dups
            seen = set()
            out_col_idxs = [x for x in last_use_single if not (x in seen or seen.add(x))]
        else:
            out_col_idxs = list(range(len(col_names)))  # Default: all columns

        out_colnames = [col_names[i] for i in out_col_idxs]
        debug(f"Output columns: {out_colnames}")

        print(",".join(out_colnames))
        for n, row in enumerate(rows):
            try:
                outrow = [row[i] for i in out_col_idxs]
                print(",".join(outrow))
                debug(f"Row {n}: {outrow}")
            except Exception as e:
                debug(f"Error printing row {n}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("--debug", action="store_true", help="Print debug info to stderr")
    parser.add_argument("inputs", nargs="+")
    args = parser.parse_args()

    global DEBUG
    DEBUG = args.debug

    specs, ops = parse_config(args.config)
    delim = specs.get("delim", ",")
    strdelim = specs.get("str", '"')

    for csvfile in args.inputs:
        debug(f"Processing input: {csvfile}")
        process_csv(csvfile, specs, ops, global_delim=delim, global_strdelim=strdelim)
