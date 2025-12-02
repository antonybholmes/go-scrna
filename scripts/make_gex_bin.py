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

VERSION = 1


def write_entries(block: int, genes: int, buffer: bytes):
    fout = path.join(dir, f"gex_{block}.bin")

    with open(fout, "wb") as f:
        f.write(struct.pack("<I", 42))  # magic
        f.write(struct.pack("<I", VERSION))  # version
        f.write(struct.pack("<I", genes))  # number of records
        # f.write(struct.pack("<I", len(offsets)))  # number of entries
        # for offset in offsets:
        #    f.write(struct.pack("<I", offset[0]))  # 4 bytes each offset
        #    f.write(struct.pack("<I", offset[1]))  # 4 bytes each size
        f.write(buffer)


parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="name")
parser.add_argument("-i", "--institution", help="institution")
parser.add_argument("-s", "--species", help="species", default="Human")
parser.add_argument("-a", "--assembly", help="assembly", default="GRCh38")
parser.add_argument("-d", "--dir", help="dir")
parser.add_argument("-c", "--cells", help="cells")
parser.add_argument("-l", "--clusters", help="clusters")
parser.add_argument("-f", "--file", help="file")
parser.add_argument("-b", "--blocksize", help="block size", default=2048, type=int)
parser.add_argument("-m", "--minexp", help="minimum expression", default=1, type=float)


args = parser.parse_args()
file = args.file
dir = args.dir
name = args.name
institution = args.institution
species = args.species
assembly = args.assembly
gex_dir = os.path.join(dir, "gex")
block_size = args.blocksize
min_exp = args.minexp

# BLOCK_SIZE = 4096  # 2^16 256
print(block_size, file=sys.stderr)

df_hugo = pd.read_csv(
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/hugo/hugo_20240524.tsv",
    sep="\t",
    header=0,
    keep_default_na=False,
)

official_symbols = {}

gene_ids = []
gene_id_map = {}
prev_gene_id_map = {}
alias_gene_id_map = {}
gene_db_map = {}

for i, gene_symbol in enumerate(df_hugo["Approved symbol"].values):

    # genes = [gene_id] + list(
    #     filter(
    #         lambda x: x != "",
    #         [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")],
    #     )
    # )

    hugo = df_hugo["HGNC ID"].values[i]
    ensembl = df_hugo["Ensembl gene ID"].values[i].split(".")[0]
    refseq = df_hugo["RefSeq IDs"].values[i].replace(" ", "")
    ncbi = df_hugo["NCBI Gene ID"].values[i].replace(" ", "")

    official_symbols[hugo] = {
        "hugo": hugo,
        "mgi": "",
        "gene_symbol": gene_symbol,
        "ensembl": ensembl,
        "refseq": refseq,
        "ncbi": ncbi,
    }

    gene_id_map[hugo] = hugo
    gene_id_map[gene_symbol] = hugo
    gene_id_map[ensembl] = hugo
    gene_id_map[refseq] = hugo
    gene_id_map[ncbi] = hugo

    for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
        prev_gene_id_map[g] = hugo

    for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
        alias_gene_id_map[g] = hugo

    index = i + 1
    gene_db_map[hugo] = index
    # gene_db_map[gene_symbol] = index
    # gene_db_map[refseq] = index
    # gene_db_map[ncbi] = index

    # for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    # for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    gene_ids.append(hugo)


df_cells = pd.read_csv(args.cells, sep="\t", header=0)
df_clusters = pd.read_csv(args.clusters, sep="\t", header=0)

cell_idx_in_use = np.where(df_cells["Cluster"].isin(df_clusters["Cluster"].values))[0]
cells_not_in_use = np.where(~df_cells["Cluster"].isin(df_clusters["Cluster"].values))[0]

df_cells = df_cells[df_cells["Cluster"].isin(df_clusters["Cluster"].values)]

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

# maximum number of cells
cells = df_cells.shape[0]
print("Cells", cells)


genes = 0
buf = bytearray()

for line in f:
    tokens = line.decode().strip().split("\t")
    g = tokens[0]

    gids = g.split(";")

    # print(g)
    # ensembl, symbol = g.split(";")

    hugo = ""

    for gid in gids:
        if gid in gene_id_map:
            hugo = gene_id_map[gid]
            break

        if gid in alias_gene_id_map:
            hugo = alias_gene_id_map[gid]
            break

        if gid in prev_gene_id_map:
            hugo = prev_gene_id_map[gid]
            break

    if hugo == "":
        # print(f"reject, unknown gene {g}", file=sys.stderr)
        continue

    # print(f"accept {g} as {hugo}", file=sys.stderr)

    official = official_symbols[hugo]
    ensembl = official["ensembl"]
    symbol = official["gene_symbol"]

    data = np.array([float(x) for x in tokens[1:]])

    # keep only cells in use
    data = data[cell_idx_in_use]

    s = np.sum(data)

    if s == 0:
        # print("reject, no exp", g)
        continue

    # reject if not expressed above min_exp in any cell
    data[data < min_exp] = 0

    # keep only non-zero entries
    idx = np.where(data > 0)[0]

    # if idx.size < data.size:
    #    print("reject, not enough cells", idx.size, data.size)
    pairs = [[float(i), float(data[i])] for i in idx]
    values = [x for pair in pairs for x in pair]

    # if len(idx) < 20:
    # print("reject, not enough cells", g)
    #    continue

    # sparse_matrix = sparse.coo_matrix(data)
    # # row unnecessary as we are looking at individual rows

    # # we record only the columns with non-zero values
    # sparse_data = [
    #     [int(c), round(float(v), 4)]
    #     for r, c, v in zip(sparse_matrix.row, sparse_matrix.col, sparse_matrix.data)
    # ]

    # flatten
    # sparse_data = [item for sublist in sparse_data for item in sublist]

    gene_id_bytes = ensembl.encode("utf-8")
    gene_symbol_bytes = symbol.encode("utf-8")
    num_values = len(values)
    total_length = (
        2 + len(gene_id_bytes) + 2 + len(gene_symbol_bytes) + 4 + num_values * 4
    )

    # print(total_length)
    # sys.exit(0)

    buf += struct.pack("<I", total_length)

    # Gene ID
    buf += struct.pack("<H", len(gene_id_bytes))
    buf += gene_id_bytes

    # Gene symbol
    buf += struct.pack("<H", len(gene_symbol_bytes))
    buf += gene_symbol_bytes

    # Number of float values
    buf += struct.pack("<I", num_values)

    # Float values
    buf += struct.pack("<" + "f" * len(values), *values)  # values.tobytes()

    # if out["s"] == "AHR":
    #     print("AHR", out)

    #     with open("AHR.json", "w") as f:
    #         json.dump(out, f)

    #     df = pd.DataFrame(sparse_data, columns=["Cell", "Exp"])
    #     df.to_csv("AHR.tsv", sep="\t", index=False)

    genes += 1

    # bunch genes into blocks of 4096 genes
    if genes == block_size:
        # fout = path.join(dir, f"gex_{block}.json.gz")
        # with gzip.open(fout, "wt", encoding="utf-8") as f:
        #     json.dump(genes, f)

        print(f"block {block} with {genes} genes")

        size = struct.unpack_from("<I", buf, 0)[0]

        print("First gene size in block:", size)

        # sys.exit(0)

        write_entries(block, genes, buf)

        # fout = path.join(dir, f"gex_{block}.dat")

        # with open(fout, "wb") as f:
        #     f.write(struct.pack("<B", 42))  # magic
        #     f.write(struct.pack("<I", len(offsets)))  # number of entries

        #     # write the offset and size of each msgpack object
        #     # in the file
        #     for offset in offsets:
        #         f.write(
        #             struct.pack("<I", offset[0])
        #         )  # where to find a msgpack bytes each offset
        #         f.write(
        #             struct.pack("<I", offset[1])
        #         )  # how much to read to decode the msgpack object 4 bytes each size

        #     f.write(buffer)

        genes = 0
        buf = bytearray()
        block += 1
        # break

    c += 1

    if c % 1000 == 0:
        print(c, file=sys.stderr)

f.close()

# write any remaining genes
if genes > 0:
    write_entries(block, genes, buf)
