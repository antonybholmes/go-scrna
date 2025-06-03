CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL, 
	institution TEXT NOT NULL, 
	species TEXT NOT NULL, 
	assembly TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	dir TEXT NOT NULL
);

CREATE TABLE cells (
	id INTEGER PRIMARY KEY ASC,
	barcode	TEXT NOT NULL, 
	umap_x REAL NOT NULL, 
	umap_y REAL NOT NULL, 
	cluster INTEGER NOT NULL, 
	sc_class TEXT NOT NULL, 
	sample TEXT NOT NULL
);

-- CREATE INDEX cells_barcode_idx ON cells (barcode);
CREATE INDEX cells_cluster_idx ON cells (cluster);
CREATE INDEX cells_sc_class_idx ON cells (sc_class);
CREATE INDEX cells_sample_idx ON cells (sample);


CREATE TABLE gex (
	id INTEGER PRIMARY KEY ASC,
	ensembl_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL, 
	file);

CREATE INDEX gex_ensembl_id_idx ON gex (ensembl_id);
CREATE INDEX gex_gene_symbol_idx ON gex (gene_symbol);


