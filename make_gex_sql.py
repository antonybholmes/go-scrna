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
parser.add_argument("-c", "--cells", help="cells")
parser.add_argument("-l", "--clusters", help="clusters")

args = parser.parse_args()
dir = args.dir
name = args.name
institution = args.institution
species = args.species
assembly = args.assembly
gex_dir = os.path.join(dir, "gex")

public_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

df_cells = pd.read_csv(args.cells, sep="\t", header=0)
df_clusters = pd.read_csv(args.clusters, sep="\t", header=0)

# use cells to count cells in each cluster
counts = []

for c in df_clusters["Cluster"].values:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)

df_clusters["Cells"] = counts





with open(os.path.join(dir, "dataset.sql"), "w") as sqlf:

    print(
        f"INSERT INTO dataset (public_id, name, institution, species, assembly, dir) VALUES ('{public_id}', '{name}', '{institution}', '{species}', '{assembly}', '{dir}');",
        file=sqlf,
    )

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
        print(
            f"INSERT INTO samples (name) VALUES ('{sample}');",
            file=sqlf,
        ),

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, row in df_clusters.iterrows():
        print(
            f"INSERT INTO clusters (cluster_id, sc_group, sc_class, cell_count, color) VALUES ({row["Cluster"]}, '{row["Group"]}', '{row["scClass"]}', {row["Cells"]}, '{row["Color"]}');",
            file=sqlf,
        ),

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, row in df_cells.iterrows():
        print(
            f"INSERT INTO cells (barcode, umap_x, umap_y, cluster_id, sample) VALUES ('{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]}, {row["Cluster"]},'{row["Sample"]}');",
            file=sqlf,
        ),

    print("COMMIT;", file=sqlf)

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
