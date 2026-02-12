import collections
import json
import os
import sqlite3
import struct

import pandas as pd
import uuid_utils as uuid

DAT_INDEX_SIZE = 256 * 4
DAT_OFFSET = 1 + 4 + DAT_INDEX_SIZE

DIR = "../data/modules/scrna"

# parser = argparse.ArgumentParser()
# parser.add_argument("-n", "--name", help="name")
# parser.add_argument("-i", "--institution", help="institution")
# parser.add_argument("-g", "--genome", help="genome", default="Human")
# parser.add_argument("-a", "--assembly", help="assembly", default="GRCh38")
# parser.add_argument("-d", "--dir", help="dir")
# parser.add_argument("-c", "--cells", help="cells")
# parser.add_argument("-l", "--clusters", help="clusters")

# args = parser.parse_args()
# dir = args.dir
# name = args.name
# institution = args.institution
# genome = args.genome
# assembly = args.assembly
# gex_dir = os.path.join(dir, "gex")


#
# Read gene symbols for matching
#

official_symbols = {"human": {}, "mouse": {}}

gene_ids = {"human": [], "mouse": []}
gene_id_map = {"human": {}, "mouse": {}}
prev_gene_id_map = {"human": {}, "mouse": {}}
alias_gene_id_map = {"human": {}, "mouse": {}}

metadata_map = {}

# gene_db_map = {}

file = (
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/hugo/hugo_20240524.tsv"
)
df_hugo = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

gene_index = 1

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

    info = {
        "index": gene_index,
        "gene_id": hugo,
        "gene_symbol": gene_symbol,
        "ensembl": ensembl,
        "refseq": refseq,
        "ncbi": ncbi,
    }

    official_symbols["human"][hugo] = info

    gene_id_map["human"][hugo] = hugo
    gene_id_map["human"][gene_symbol] = hugo
    gene_id_map["human"][ensembl] = hugo
    gene_id_map["human"][refseq] = hugo
    gene_id_map["human"][ncbi] = hugo
    for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
        prev_gene_id_map["human"][g] = hugo

    for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
        alias_gene_id_map["human"][g] = hugo

    # gene_db_map[hugo] = hugo  # index
    # gene_db_map[gene_symbol] = index
    # gene_db_map[refseq] = index
    # gene_db_map[ncbi] = index

    # for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    # for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    gene_ids["human"].append(hugo)
    gene_index += 1

file = "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/mgi_entrez_ensembl_gene_list_20240531.tsv"
df_mgi = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

for i, gene_symbol in enumerate(df_mgi["gene_symbol"].values):

    mgi = df_mgi["mgi"].values[i]
    ensembl = df_mgi["ensembl"].values[i].split(".")[0].replace("null", "")
    refseq = df_mgi["refseq"].values[i].replace(" ", "").replace("null", "")
    ncbi = df_mgi["entrez"].values[i].replace(" ", "").replace("null", "")

    official_symbols["mouse"][mgi] = {
        "index": gene_index,
        "gene_id": mgi,
        "gene_symbol": gene_symbol,
        "ensembl": ensembl,
        "refseq": refseq,
        "ncbi": ncbi,
    }

    gene_id_map["mouse"][mgi] = mgi
    gene_id_map["mouse"][gene_symbol] = mgi
    gene_id_map["mouse"][refseq] = mgi
    gene_id_map["mouse"][ncbi] = mgi

    gene_index += 1
    # gene_db_map[mgi] = index
    # gene_db_map[gene_symbol] = index
    # gene_db_map[refseq] = index
    # gene_db_map[ncbi] = index

    gene_ids["mouse"].append(mgi)


#
# load datasets
#

with open("datasets.json") as f:
    datasets = json.load(f)

print(datasets)


db = os.path.join(DIR, "scrna.db")


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f"""
    CREATE TABLE genomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        scientific_name TEXT NOT NULL,
        UNIQUE(name, scientific_name));
    """,
)

cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (1, '{uuid.uuid7()}', 'Human', 'Homo sapiens');"
)
cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (2, '{uuid.uuid7()}', 'Mouse', 'Mus musculus');"
)

genome_map = {"Human": 1, "Mouse": 2}

cursor.execute(
    f"""
    CREATE TABLE assemblies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        UNIQUE(genome_id, name),
        FOREIGN KEY(genome_id) REFERENCES genomes(id));
    """,
)

cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (1, '{uuid.uuid7()}', 1, 'GRCh38');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (2, '{uuid.uuid7()}', 2, 'GRCm39');"
)

assemblies_map = {"GRCh38": 1, "GRCm39": 2}


cursor.execute(
    f"""
    CREATE TABLE genes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        gene_id TEXT NOT NULL,
        ensembl TEXT NOT NULL DEFAULT '',
        refseq TEXT NOT NULL DEFAULT '',
        ncbi INTEGER NOT NULL DEFAULT 0,
        gene_symbol TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(genome_id) REFERENCES genomes(id));
    """,
)

genomes = ["Human", "Mouse"]

genome_map = {"Human": 1, "Mouse": 2}

for si, g in enumerate(genomes):
    genome_id = si + 1
    for id in sorted(official_symbols[g.lower()]):
        d = official_symbols[g.lower()][id]

        cursor.execute(
            f"INSERT INTO genes (id, public_id, genome_id, gene_id, ensembl, refseq, ncbi, gene_symbol) VALUES (:id, :public_id, :genome_id, :gene_id, :ensembl, :refseq, :ncbi, :gene_symbol);",
            (
                {
                    "id": d["index"],
                    "public_id": str(uuid.uuid7()),
                    "genome_id": genome_id,
                    "gene_id": d["gene_id"],
                    "ensembl": d["ensembl"],
                    "refseq": d["refseq"],
                    "ncbi": d["ncbi"],
                    "gene_symbol": d["gene_symbol"],
                }
            ),
        )

cursor.execute("COMMIT;")


cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE TABLE datasets (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	assembly_id INTEGER NOT NULL,
    name TEXT NOT NULL, 
	institution TEXT NOT NULL, 
	cells INTEGER NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(assembly_id) REFERENCES assemblies(id)
);
"""
)

cursor.execute(
    f""" CREATE TABLE permissions (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f""" CREATE TABLE dataset_permissions (
	dataset_id INTEGER,
    permission_id INTEGER,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id));
"""
)

rdfViewId = str(uuid.uuid7())

cursor.execute(
    f"INSERT INTO permissions (id, public_id, name) VALUES (1, '{rdfViewId}', 'rdf:view');"
)

cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
	FOREIGN KEY(dataset_id) REFERENCES datasets(id)
);
"""
)

# cursor.execute(
#     f""" CREATE TABLE metadata_types (
# 	id INTEGER PRIMARY KEY,
#     uuid TEXT NOT NULL UNIQUE,
# 	name TEXT NOT NULL,
# 	description TEXT NOT NULL DEFAULT '',
# 	UNIQUE(name));
# """
# )

cursor.execute(
    f""" CREATE TABLE metadata (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT '');
"""
)

cursor.execute(
    f""" CREATE TABLE clusters (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    dataset_id INTEGER NOT NULL,
    label INTEGER NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	cell_count INTEGER NOT NULL,
	color TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(dataset_id) REFERENCES datasets(id)
);
"""
)

cursor.execute(
    f""" CREATE TABLE cluster_metadata (
	cluster_id INTEGER NOT NULL,
	metadata_id INTEGER NOT NULL,
    value TEXT NOT NULL,
	PRIMARY KEY(cluster_id, metadata_id),
	FOREIGN KEY(cluster_id) REFERENCES clusters(id),
	FOREIGN KEY(metadata_id) REFERENCES metadata(id)  
);
"""
)

cursor.execute(
    f""" CREATE TABLE cells (
    id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    dataset_id INTEGER NOT NULL,
	sample_id INTEGER NOT NULL,
    cluster_id INTEGER NOT NULL, 
	barcode	TEXT NOT NULL, 
	umap_x REAL NOT NULL, 
	umap_y REAL NOT NULL,
    UNIQUE(dataset_id, sample_id, cluster_id, barcode),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
	FOREIGN KEY (cluster_id) REFERENCES clusters(id),
	FOREIGN KEY (sample_id) REFERENCES samples(id)  
);
"""
)

cursor.execute(
    f"""
    CREATE TABLE gex_types (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '');
    """,
)

cursor.execute(
    f"INSERT INTO gex_types (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'Counts');"
)
cursor.execute(
    f"INSERT INTO gex_types (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'CPM');"
)

cursor.execute(
    f"INSERT INTO gex_types (id, public_id, name) VALUES (3, '{uuid.uuid7()}', 'log1p(CPM)');"
)

cursor.execute(
    f"INSERT INTO gex_types (id, public_id, name, description) VALUES (4, '{uuid.uuid7()}', 'Normalized', 'Seurat log1p((counts / total_counts_per_cell) * 10000)');"
)

expression_type_map = {"Counts": 1, "CPM": 2, "log1p(CPM)": 3, "Normalized": 4}

cursor.execute(
    f"""
    CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        url TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute(
    f""" CREATE TABLE gex (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    gene_id INTEGER NOT NULL,
    gex_type_id INTEGER NOT NULL,
    dataset_id INTEGER NOT NULL,
	offset INTEGER NOT NULL,
	size INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(gex_type_id) REFERENCES gex_types(id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY(gene_id) REFERENCES genes(id),
    FOREIGN KEY(file_id) REFERENCES files(id)
);
"""
)


cursor.execute("COMMIT;")

metadata_type_map = {}
file_map = {}

expression_id = 1

used_gene_ids = {}

for di, dataset in enumerate(datasets):
    dataset_index = di + 1
    dataset_id = str(
        uuid.uuid7()
    )  # = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

    df_cells = pd.read_csv(dataset["cells"], sep="\t", header=0)
    df_clusters = pd.read_csv(dataset["clusters"], sep="\t", header=0, index_col=0)

    # get rid of clusters 101 etc
    df_cells = df_cells[df_cells["Cluster"].isin(df_clusters.index)]

    # use cells to count cells in each cluster
    counts = []

    for c in df_clusters.index:
        count = len(df_cells[df_cells["Cluster"] == c])
        counts.append(count)

    # map cluster id to uuid e.g. 1 -> 'c4f8e2a0-1d5b-11ee-be56-0242ac120002'
    cluster_id_map = {
        c: {"uuid": uuid.uuid7(), "index": i + 1}
        for i, c in enumerate(df_clusters.index)
    }

    # df_clusters["Cells"] = counts

    cursor.execute("BEGIN TRANSACTION;")

    cursor.execute(
        f"""INSERT INTO datasets (id, public_id,  assembly_id, name, institution, cells) VALUES (
            {dataset_index}, 
            '{dataset_id}', 
            {assemblies_map[dataset["assembly"]]},
            '{dataset["name"]}', 
            '{dataset["institution"]}',
            {df_cells.shape[0]});
        """,
    )

    cursor.execute(
        f"""INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES 
                (:dataset_id, :permission_id);""",
        {"dataset_id": dataset_index, "permission_id": 1},
    )

    sample_map = {}

    for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
        sample_map[sample] = {"uuid": uuid.uuid7(), "index": i + 1}

        cursor.execute(
            f"""INSERT INTO samples (id, public_id, dataset_id, name) VALUES (
                {sample_map[sample]["index"]}, 
                '{sample_map[sample]["uuid"]}', 
                {dataset_index}, 
                '{sample}'
            );""",
        )

    cursor.execute("COMMIT;")

    cursor.execute("BEGIN TRANSACTION;")

    for idx, (cluster, row) in enumerate(df_clusters.iterrows()):
        cluster_id = cluster_id_map[row.name]["uuid"]

        # row name is the cluster label, a number
        label = int(row.name)
        cursor.execute(
            f"""INSERT INTO clusters (id, public_id, dataset_id, label, name, cell_count, color) VALUES (
                {idx + 1}, 
                '{cluster_id}', 
                {dataset_index},
                {label}, 
                '{cluster}',  
                {counts[idx]}, 
                '{row["Color"]}'
            );""",
        )

    cursor.execute("COMMIT;")

    # cursor.execute("BEGIN TRANSACTION;")

    # for i, name in enumerate(metadata_types):
    #     metadata_id = metadata_type_map[name]  # uuid.uuid7()
    #     cursor.execute(
    #         f"INSERT INTO metadata_types (id, uuid, name) VALUES ({i + 1}, '{metadata_id}', '{name}');",
    #     )

    # cursor.execute("COMMIT;")

    cursor.execute("BEGIN TRANSACTION;")

    metadata_map = collections.defaultdict(lambda: {})

    metadata_types = list(sorted(df_clusters.columns[1:].values))

    for i, name in enumerate(metadata_types):
        if name not in metadata_type_map:
            metadata_type_map[name] = {
                "uuid": uuid.uuid7(),
                "index": len(metadata_type_map) + 1,
            }

            metadata_type_id = metadata_type_map[name]["uuid"]
            idx = metadata_type_map[name]["index"]
            cursor.execute(
                f"INSERT INTO metadata (id, public_id, name) VALUES ({idx}, '{metadata_type_id}',  '{name}');",
            )

    cursor.execute("COMMIT;")

    cursor.execute("BEGIN TRANSACTION;")

    # clusters can have metadata attached to them
    for idx, (i, row) in enumerate(df_clusters.iterrows()):
        cluster_index = cluster_id_map[row.name]["index"]
        cluster_id = cluster_id_map[row.name]["uuid"]

        for j, metadata_type in enumerate(metadata_types):
            metadata_index = metadata_type_map[metadata_type]["index"]
            metadata_value = row[j + 1]

            cursor.execute(
                f"INSERT INTO cluster_metadata (cluster_id, metadata_id, value) VALUES ({cluster_index}, {metadata_index}, '{metadata_value}');",
            )

    cursor.execute("COMMIT;")

    cursor.execute("BEGIN TRANSACTION;")

    for i, row in df_cells.iterrows():
        cell_id = uuid.uuid7()
        sample = row["Sample"]
        sample_id = sample_map[sample]["index"]
        cluster_id = cluster_id_map[row["Cluster"]]["index"]

        # print(dataset_index, sample_id, cluster_id)

        cursor.execute(
            f"INSERT INTO cells (public_id, dataset_id, sample_id, cluster_id, barcode, umap_x, umap_y) VALUES ('{cell_id}', {dataset_index}, {sample_id}, {cluster_id}, '{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]});",
        )

    cursor.execute("COMMIT;")

    cursor.execute("BEGIN TRANSACTION;")

    root_dir = dataset["root"]

    for file in dataset["data"]:
        type = file["type"]
        type_id = expression_type_map[type]

        relative_dir = file["path"]
        dir = os.path.join(root_dir, relative_dir)

        for f in sorted(os.listdir(dir)):
            # if f.endswith(".json.gz"):
            #     # cursor.execute(f)
            #     with gzip.open(os.path.join(gex_dir, f), "r") as fin:
            #         data = json.load(fin)

            #         for d in data:
            #             id = d["id"]
            #             sym = d["sym"]

            #             cursor.execute(
            #                 f"INSERT INTO gex (ensembl_id, gene_symbol, file, offset) VALUES ('{id}', '{sym}', '{f}');",
            #                 ,
            #             )

            if f.endswith(".gex"):
                # cursor.execute(f)
                relative_file = os.path.join(relative_dir, f)
                file = os.path.join(root_dir, relative_file)

                if relative_file not in file_map:
                    file_id = uuid.uuid7()
                    file_map[relative_file] = {
                        "uuid": file_id,
                        "index": len(file_map) + 1,
                    }

                    cursor.execute(
                        f"INSERT INTO files (id, public_id, url) VALUES ({file_map[relative_file]['index']}, '{file_id}', '{relative_file}');",
                    )

                file_id = file_map[relative_file]["index"]

                with open(file, "rb") as fin:

                    magic = struct.unpack("<I", fin.read(4))[0]
                    print("Magic:", file, magic)

                    # Step 1: Read the offset table entry

                    version = struct.unpack("<I", fin.read(4))[0]
                    print("Version:", version)

                    cells = struct.unpack("<I", fin.read(4))[0]
                    print("Cells:", cells)

                    # num genes
                    # num_entries = struct.unpack("<I", fin.read(4))[0]

                    # each entry is 8 bytes (4 bytes offset, 4 bytes size)
                    # data = fin.read(num_entries * 4 * 2)

                    # Unpack as  unsigned ints (little-endian)
                    # offsets = struct.unpack(f"<{num_entries*2}I", data)

                    print(f, cells)

                    # magic + version + num entries
                    dat_offset = 4 + 4 + 4

                    for i in range(0, cells):
                        # size of object
                        size = struct.unpack("<I", fin.read(4))[0]

                        ensembl_id_len = struct.unpack("<H", fin.read(2))[0]
                        ensembl_id = fin.read(ensembl_id_len).decode("utf-8")

                        gene_symbol_len = struct.unpack("<H", fin.read(2))[0]
                        gene_symbol = fin.read(gene_symbol_len).decode("utf-8")

                        keep = True

                        if ensembl_id in gene_id_map[dataset["genome"].lower()]:
                            gene_id = gene_id_map[dataset["genome"].lower()][ensembl_id]

                        elif ensembl_id in prev_gene_id_map[dataset["genome"].lower()]:
                            gene_id = prev_gene_id_map[dataset["genome"].lower()][
                                ensembl_id
                            ]

                            print(
                                f"Gene symbol {ensembl_id} not found in official symbols, but found in previous symbols as {gene_symbol}"
                            )
                        elif ensembl_id in alias_gene_id_map[dataset["genome"].lower()]:
                            gene_id = alias_gene_id_map[dataset["genome"].lower()][
                                ensembl_id
                            ]

                            print(
                                f"Gene symbol {ensembl_id} not found in official symbols, but found in alias symbols as {gene_id}"
                            )
                        else:
                            print(
                                f"Gene symbol {ensembl_id} not found in official symbols, previous symbols, or alias symbols for genome {dataset['genome']}"
                            )
                            keep = False

                        gene_index = official_symbols[dataset["genome"].lower()][
                            gene_id
                        ]["index"]

                        if gene_index in used_gene_ids:
                            print(
                                f"xene {gene_symbol} with gene id {gene_id} {gene_index} already used, new: {ensembl_id} old: {used_gene_ids[gene_index]}"
                            )

                        print("Size:", size, ensembl_id, gene_symbol, file)

                        if keep:
                            # now we can get the id and gene symbol
                            gex_id = uuid.uuid7()

                            # log the offset and size in the db so we can search
                            # for a gene and then know where to find it in the file
                            cursor.execute(
                                f"""INSERT INTO gex (id, public_id, dataset_id, gene_id, gex_type_id, offset, size, file_id) VALUES (
                                        {expression_id}, 
                                        '{gex_id}', 
                                        {dataset_index}, 
                                        {gene_index}, 
                                        {type_id}, 
                                        {dat_offset}, 
                                        {size},
                                        {file_id});""",
                            )

                            expression_id += 1

                        # size does not include the 4 bytes of size itself
                        # so we must add it to get to the next record
                        dat_offset += size

                        # skip in file to next record
                        fin.seek(dat_offset, 0)

                        used_gene_ids[gene_index] = ensembl_id

    cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE INDEX genomes_name_idx ON genomes (LOWER(name));
"""
)

cursor.execute(
    f""" CREATE INDEX assemblies_name_idx ON assemblies (LOWER(name));
"""
)

cursor.execute(
    f""" CREATE INDEX clusters_name_idx ON clusters (LOWER(name));
"""
)

cursor.execute(
    f""" CREATE INDEX cells_barcode_idx ON cells (barcode);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_sample_id_idx ON cells (sample_id);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_dataset_id_idx ON cells (dataset_id);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_cluster_id_idx ON cells (cluster_id);
"""
)


cursor.execute("COMMIT;")
