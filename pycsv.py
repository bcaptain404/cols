#!/usr/bin/env python3

import csv
import argparse
import sys
import re

def parse_config(cfg_path):
    ops = []
    specs = {}
    with open(cfg_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Split specs (e.g. "in head 5")
            if line.startswith("in "):
                keyval = line[3:].split()
                if keyval:
                    specs[keyval[0]] = keyval[1] if len(keyval) > 1 else True
                continue
            ops.append(line)
    return specs, ops

def parse_col(ref, header):
    # Accepts @5 or @"Name"
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
    with open(input_path, newline='') as f:
        delim = specs.get('delim', global_delim)
        strdelim = specs.get('str', global_strdelim)
        reader = csv.reader(f, delimiter=delim, quotechar=strdelim)

        header = next(reader)
        header_original = list(header)
        header_pos = {name: i for i, name in enumerate(header)}
        col_strdelim = {i: strdelim for i in range(len(header))}

        col_idxs = list(range(len(header)))
        col_names = list(header)
        out_rows = []

        rows = []
        for row in reader:
            rows.append(list(row))

        if 'skip' in specs:
            rows = rows[int(specs['skip']):]
        if 'head' in specs:
            rows = rows[:int(specs['head'])]
        if 'tail' in specs:
            rows = streaming_tail(rows, int(specs['tail']))
        if 'trunc' in specs:
            rows = streaming_trunc(rows, int(specs['trunc']))
        if 'max' in specs:
            rows = rows[:int(specs['max'])]

        for op in ops:
            tokens = re.findall(r'@[0-9]+|@"[^"]+"|".+?"|\S+', op)
            cmd = tokens[0]
            args = tokens[1:]

            if cmd == "use":
                idx = parse_col(args[0], col_names)
                if idx is None or idx not in col_idxs:
                    continue  # skip if missing
                col_idxs = [i for i in col_idxs if i == idx] + [i for i in col_idxs if i != idx]
            elif cmd == "rn":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    continue
                newname = args[1].strip('"')
                col_names[idx] = newname
            elif cmd == "add":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    continue
                col_names.insert(idx, args[1].strip('"'))
                for row in rows:
                    row.insert(idx, "")
                col_idxs = list(range(len(col_names)))
            elif cmd == "set":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    continue
                val = args[1].strip('"')
                for row in rows:
                    row[idx] = val
            elif cmd == "replace_all":
                find, repl = args[0].strip('"'), args[1].strip('"')
                col_names = [do_replace(x, find, repl) for x in col_names]
                for row in rows:
                    for j, cell in enumerate(row):
                        row[j] = do_replace(cell, find, repl)
            elif cmd == "replace_head":
                find, repl = args[0].strip('"'), args[1].strip('"')
                col_names = [do_replace(x, find, repl) for x in col_names]
            elif cmd == "replace_cell":
                find, repl = args[0].strip('"'), args[1].strip('"')
                for row in rows:
                    for j, cell in enumerate(row):
                        row[j] = do_replace(cell, find, repl)
            elif cmd == "replace":
                idx = parse_col(args[0], col_names)
                if idx is None:
                    continue
                find, repl = args[1].strip('"'), args[2].strip('"')
                for row in rows:
                    row[idx] = do_replace(row[idx], find, repl)
            elif cmd == "move":
                a = parse_col(args[0], col_names)
                b = parse_col(args[1], col_names)
                if a is None or b is None:
                    continue
                do_move(col_names, a, b)
                for row in rows:
                    do_move(row, a, b)
                col_idxs = list(range(len(col_names)))
            elif cmd == "swap":
                a = parse_col(args[0], col_names)
                b = parse_col(args[1], col_names)
                if a is None or b is None:
                    continue
                do_swap(col_names, a, b)
                for row in rows:
                    do_swap(row, a, b)
                col_idxs = list(range(len(col_names)))
            elif cmd == "in" and args[0] == "col" and args[2] == "str":
                idx = int(args[1])
                if idx is None:
                    continue
                col_strdelim[idx] = args[3].strip('"')
            else:
                if cmd not in ("in",):
                    print(f"Warning: Unrecognized op '{op}'", file=sys.stderr)

        out_colnames = [col_names[i] for i in col_idxs]
        print(",".join(out_colnames))
        for row in rows:
            outrow = [row[i] for i in col_idxs]
            print(",".join(outrow))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("inputs", nargs="+")
    args = parser.parse_args()

    specs, ops = parse_config(args.config)
    delim = specs.get("delim", ",")
    strdelim = specs.get("str", '"')

    for csvfile in args.inputs:
        process_csv(csvfile, specs, ops, global_delim=delim, global_strdelim=strdelim)
