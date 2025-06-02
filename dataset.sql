CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL, 
	institution TEXT NOT NULL, 
	species TEXT NOT NULL, 
	assembly TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	gex_dir TEXT NOT NULL
);

CREATE TABLE gex (
	id INTEGER PRIMARY KEY ASC,
	ensembl_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL, 
	file);

CREATE INDEX gex_ensembl_id_idx ON gex (ensembl_id);
CREATE INDEX gex_gene_symbol_idx ON gex (gene_symbol);


