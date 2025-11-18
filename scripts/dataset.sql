CREATE TABLE dataset (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL, 
	institution TEXT NOT NULL, 
	species TEXT NOT NULL, 
	assembly TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	cells INTEGER NOT NULL,
	tags TEXT NOT NULL DEFAULT '',
	dir TEXT NOT NULL
);


CREATE TABLE samples (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL UNIQUE
);

CREATE TABLE metadata_types (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	UNIQUE(name));

CREATE TABLE metadata (
	id TEXT PRIMARY KEY ASC,
	metadata_type_id TEXT NOT NULL,
	value TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	color TEXT NOT NULL DEFAULT '',
	UNIQUE(metadata_type_id, value, color),
	FOREIGN KEY(metadata_type_id) REFERENCES metadata_types(id));

CREATE TABLE clusters (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL UNIQUE,
	cell_count INTEGER NOT NULL,
	color TEXT NOT NULL DEFAULT ''
);
CREATE INDEX clusters_name_idx ON clusters (name);

CREATE TABLE cluster_metadata (
	id TEXT PRIMARY KEY ASC,
	cluster_id TEXT NOT NULL,
	metadata_id TEXT NOT NULL,
	UNIQUE(cluster_id, metadata_id),
	FOREIGN KEY(cluster_id) REFERENCES clusters(id),
	FOREIGN KEY(metadata_id) REFERENCES metadata(id)  
);


CREATE TABLE cells (
	id INTEGER PRIMARY KEY ASC,
	barcode	TEXT NOT NULL, 
	umap_x REAL NOT NULL, 
	umap_y REAL NOT NULL, 
	cluster_id TEXT NOT NULL, 
	sample_id TEXT NOT NULL,
	FOREIGN KEY (cluster_id) REFERENCES clusters(id),
	FOREIGN KEY (sample_id) REFERENCES samples(id)  
);

CREATE INDEX cells_barcode_idx ON cells (barcode);
CREATE INDEX cells_cluster_id_idx ON cells (cluster_id);
CREATE INDEX cells_sample_id_idx ON cells (sample_id);


CREATE TABLE gex (
	id TEXT PRIMARY KEY ASC,
	ensembl_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL, 
	file TEXT NOT NULL,
	offset INTEGER NOT NULL,
	size INTEGER NOT NULL
);

CREATE INDEX gex_ensembl_id_idx ON gex (ensembl_id);
CREATE INDEX gex_gene_symbol_idx ON gex (gene_symbol);


