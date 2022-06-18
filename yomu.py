#!/usr/bin/env python

import os
import sys
import argparse
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Merge GENESIS sequence of log files")
    parser.add_argument(               dest="inp_name", metavar="FILE", nargs="+",                help="GENESIS log files")
    parser.add_argument("--out-name",  dest="out_name", metavar="NAME", nargs=1,   default="out", help="Output file name")
    parser.add_argument("--out-path",  dest="out_path", metavar="PATH", nargs=1,   default="./",  help="Output file path")
    parser.add_argument("--log-names", dest="log_name", metavar="NAME", nargs="+", default=None,  help="GENESIS log files' name")

    return parser.parse_args()


def get_file_content(file):
    file.seek(0)
    content = []
    for line in file:
        if line.startswith("INFO"):
            line = line.strip()
            content.append(line)

    return content


def get_file_metadata(file):
    file.seek(0)
    meta = {"dt": np.nan, "ensemble": "", "ff": ""}
    for line in file:
        line = line.strip()
        # Parse timestep line:
        if line.startswith("timestep"):
            tokens = line.split()
            meta["dt"] = float(tokens[2])

        # Parse force field:
        if line.startswith("forcefield"):
            tokens = line.split()
            meta["ff"] = tokens[2]

        # Parse ensemble line:
        if line.startswith("ensemble"):
            tokens = line.split()
            meta["ensemble"] = tokens[2]

    return meta


def update_dataframe(df, content, content_name, metadata):
    # Parse header
    header = content[0]
    header = header.split()
    header = ["NAME", "TIMESTEP", "FORCEFIELD", "ENSEMBLE"] + header[1:]

    # Get metadata
    dt = metadata["dt"]
    ff = metadata["ff"]
    en = metadata["ensemble"]

    # Add columns to dataframe
    for item in header:
        if item not in df:
            df[item] = ""

    # Parse content into a dataframe
    data = []
    for line in content[1:]:
        line = line.split()
        line = line[1:]
        line = [float(item) for item in line]
        line = [content_name, dt, ff, en] + line
        data.append(line)

    new_df = pd.DataFrame(data, columns=header)

    # Fix step
    if not df.empty:
        new_df["STEP"] += df["STEP"].iat[-1]

    # Fix time
    if not df.empty:
        new_df["TIME"] += df["TIME"].iat[-1]

    # Typecast step to int
    new_df = new_df.astype({"STEP": int})

    # Concatenate new dataframe
    df = pd.concat([df, new_df])

    # Fill empty values with NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)

    return df


def main():
    # Get file names
    args = parse_args()
    inp_name = args.inp_name
    log_name = args.log_name

    # Get log dataframe names
    if not log_name:
        log_name = [os.path.basename(inp)    for inp in inp_name]
        log_name = [os.path.splitext(log)[0] for log in log_name]

    # Parse log files into a dataframe
    df = pd.DataFrame()

    for inp, log in zip(inp_name, log_name):
        with open(inp, "r") as file:
            cont = get_file_content(file)
            meta = get_file_metadata(file)
            df   = update_dataframe(df, cont, log, meta)

    # Save dataframe
    out_name = args.out_name
    out_path = args.out_path
    out_type = "csv"

    os.makedirs(out_path, exist_ok=True)
    df.to_csv(os.path.join(out_path, out_name + "." + out_type), index=False)


if __name__ == "__main__":
    main()
    sys.exit(0)
