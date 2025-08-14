import argparse
import gzip
import json
from os import path
import os
from nanoid import generate
import pandas as pd
from scipy import sparse
import numpy as np
import sys
import msgpack
import struct

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="name")
parser.add_argument("-i", "--institution", help="institution")
parser.add_argument("-s", "--species", help="species", default="Human")
parser.add_argument("-a", "--assembly", help="assembly", default="GRCh38")
parser.add_argument("-d", "--dir", help="dir")
parser.add_argument("-c", "--cells", help="cells")
parser.add_argument("-l", "--clusters", help="clusters")
parser.add_argument("-f", "--file", help="file")

args = parser.parse_args()
file = args.file
dir = args.dir
name = args.name
institution = args.institution
species = args.species
assembly = args.assembly
gex_dir = os.path.join(dir, "gex")


BLOCK_SIZE = 4096  # 2^16 256


public_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

df_cells = pd.read_csv(args.cells, sep="\t", header=0)
df_clusters = pd.read_csv(args.clusters, sep="\t", header=0)

# use cells to count cells in each cluster
counts = []

for c in df_clusters["Cluster"].values:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)

df_clusters["Cells"] = counts


# print all rows msgpack

f = gzip.open(file, "r")
# skip header
f.readline()

c = 0
block = 1

genes = []

offsets = []
buffer = b""

for line in f:
    tokens = line.decode().strip().split("\t")
    g = tokens[0]
    # print(g)
    ensembl, symbol = g.split(";")

    data = np.array([float(x) for x in tokens[1:]])

    s = data.sum()

    if s == 0:
        # print("reject, no exp", g)
        continue

    # reject if not in 10 of cells
    idx = np.where(data > 0)[0]

    # if len(idx) < 20:
    # print("reject, not enough cells", g)
    #    continue

    sparse_matrix = sparse.coo_matrix(data)
    # row unnecessary as we are looking at individual rows
    sparse_data = [
        [int(c), round(float(v), 4)]
        for r, c, v in zip(sparse_matrix.row, sparse_matrix.col, sparse_matrix.data)
    ]

    # flatten
    # sparse_data = [item for sublist in sparse_data for item in sublist]

    out = {"id": ensembl, "sym": symbol, "gex": sparse_data}

    genes.append(out)

    encoded = msgpack.packb(out, use_single_float=True)

    offsets.append([len(buffer), len(encoded)])
    buffer += encoded

    # bunch genes into blocks of 32
    if len(genes) == BLOCK_SIZE:
        # fout = path.join(dir, f"gex_{block}.json.gz")
        # with gzip.open(fout, "wt", encoding="utf-8") as f:
        #     json.dump(genes, f)
        fout = path.join(dir, f"gex_{block}.dat")

        with open(fout, "wb") as f:
            f.write(struct.pack("<B", 42))  # magic
            f.write(struct.pack("<I", len(offsets)))  # number of entries
            for offset in offsets:
                f.write(struct.pack("<I", offset[0]))  # 4 bytes each offset
                f.write(struct.pack("<I", offset[1]))  # 4 bytes each size
            f.write(buffer)

        genes = []

        buffer = b""
        offsets = []

        block += 1
        # break

    c += 1

    if c % 1000 == 0:
        print(c, file=sys.stderr)

f.close()

if len(genes) > 0:
    # fout = path.join(dir, f"gex_{block}.json.gz")
    # with gzip.open(fout, "wt", encoding="utf-8") as f:
    #     json.dump(genes, f)  # , indent=2)

    fout = path.join(dir, f"gex_{block}.dat")

    with open(fout, "wb") as f:
        f.write(struct.pack("<B", 42))  # magic
        f.write(struct.pack("<I", len(offsets)))  # number of entries
        for offset in offsets:
            f.write(struct.pack("<I", offset[0]))  # 4 bytes each offset
            f.write(struct.pack("<I", offset[1]))  # 4 bytes each size
        f.write(buffer)
