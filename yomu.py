#!/usr/bin/env python

# TODO
# - parse metadata like time step or integrator and add them to the dataframe
# - output current processing step to user
# - add option to give names to each file

import os
import sys
import argparse
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Merge GENESIS sequence of log files")
    parser.add_argument("files", metavar="FILE", nargs="+", help="GENESIS log files")

    return parser.parse_args()


def get_file_content(file):
    content = []
    for line in file:
        if line.startswith("INFO"):
            line = line.strip()
            content.append(line)

    return content


def update_dataframe(df, content):
    # Parse header
    header = content[0]
    header = header.split()
    header = header[1:]

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
        data.append(line)

    new_df = pd.DataFrame(data, columns=header)

    # Fix step
    if not df.empty:
        new_df["STEP"] += df["STEP"].iat[-1]

    # Fix time
    if not df.empty:
        new_df["TIME"] += df["TIME"].iat[-1]

    # Concatenate new dataframe
    df = pd.concat([df, new_df])

    # Fill empty values with NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)

    return df


def main():
    args = parse_args()
    list = args.files

    df = pd.DataFrame()

    for file_name in list:
        with open(file_name, "r") as file:
            content = get_file_content(file)
            df = update_dataframe(df, content)

    df.to_csv("out.csv", index=False)


if __name__ == "__main__":
    main()
    sys.exit(0)
