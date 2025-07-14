#!/usr/bin/env python3

import csv
import argparse
import sys
import re

# --- Config Parsing ---
def parse_config(cfg_path):
    ops = []
    with open(cfg_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Split opcode and rest
            parts = re.split(r'\s+', line, maxsplit=1)
            opcode = parts[0]
            args = parts[1] if len(parts) > 1 else ""
            ops.append((opcode, args))
    return ops

# --- Operation Implementations ---
def parse_col(ref, header):
    # Accepts @5 or @"Name"
    if ref.startswith('@'):
        ref = ref[1:]
        if ref.startswith('"') and ref.endswith('"'):
            name = ref[1:-1]
            if name in header:
                return header.index(name)
            else:
                raise ValueError(f"Column name '{name}' not found.")
        else:
            return int(ref)
    raise ValueError(f"Bad column spec: {ref}")

def apply_ops(header, rows, ops):
    colnames = list(header)
    keep_cols = list(range(len(header)))  # Start with all columns
    renames = {}

    # You can expand this dispatcher with more ops as needed
    for op, argstr in ops:
        if op == "use":
            # use @N or @"Name"
            tokens = argstr.split()
            idx = parse_col(tokens[0], colnames)
            keep_cols = [idx] if not keep_cols else keep_cols + [idx] if idx not in keep_cols else keep_cols
        elif op == "rn":
            # rn @N "NewName" or rn @"Old" "NewName"
            tokens = re.findall(r'@[^\s]+|"[^"]+"', argstr)
            idx = parse_col(tokens[0], colnames)
            newname = tokens[1].strip('"')
            renames[idx] = newname
        # Add other ops as needed
    # Apply renames
    for idx, newname in renames.items():
        colnames[idx] = newname
    # Select columns
    out_header = [colnames[i] for i in keep_cols]
    # Output generator
    def gen_rows():
        for row in rows:
            yield [row[i] for i in keep_cols]
    return out_header, gen_rows()

# --- Main Execution ---
def process_csv(inpath, ops, delim=",", string_delim='"'):
    with open(inpath, newline='') as f:
        reader = csv.reader(f, delimiter=delim, quotechar=string_delim)
        header = next(reader)
        out_header, out_rows = apply_ops(header, reader, ops)
        writer = csv.writer(sys.stdout, delimiter=delim, quotechar=string_delim)
        writer.writerow(out_header)
        for row in out_rows:
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("inputs", nargs="+")
    args = parser.parse_args()

    ops = parse_config(args.config)
    for csvfile in args.inputs:
        process_csv(csvfile, ops)
