import collections
import gzip
import json
import os
import re
import sys
import pandas as pd
import numpy as np
from nanoid import generate

import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="name")
parser.add_argument("-i", "--institution", help="institution")
parser.add_argument("-s", "--species", help="species", default="Human")
parser.add_argument("-a", "--assembly", help="assembly", default="GRCh38")
parser.add_argument("-d", "--dir", help="dir")

args = parser.parse_args()
dir = args.dir
name = args.name
institution = args.institution
species = args.species
assembly = args.assembly
gex_dir = os.path.join(dir, "gex")

public_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)


with open(os.path.join(dir, "dataset.sql"), "w") as sqlf:

    print(
        f"INSERT INTO dataset (public_id, name, institution, species, assembly, gex_dir) VALUES ('{public_id}', '{name}', '{institution}', '{species}', '{assembly}', '{gex_dir}');",
        file=sqlf,
    )

    print("BEGIN TRANSACTION;", file=sqlf)

    for f in sorted(os.listdir(gex_dir)):
        if f.endswith(".json.gz"):
            # print(f)
            with gzip.open(os.path.join(gex_dir, f), "r") as fin:
                data = json.load(fin)

                for d in data:
                    id = d["id"]
                    sym = d["sym"]

                    print(
                        f"INSERT INTO gex (ensembl_id, gene_symbol, file) VALUES ('{id}', '{sym}', '{f}');",
                        file=sqlf,
                    )

    print("COMMIT;", file=sqlf)
