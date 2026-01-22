package scrna

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/antonybholmes/go-scrna/dat"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web/auth"
)

type (
	Idtype struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	NameValueType struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}

	Species  = Idtype
	GexValue = Idtype

	// type GexType string

	// const (
	// 	GEX_TYPE_RNA_SEQ        GexType = "RNA-seq"
	// 	GEX_TYPE_RNA_MICROARRAY GexType = "Microarray"
	//)

	Sample struct {
		Id       string          `json:"id"`
		Name     string          `json:"name"`
		AltNames []string        `json:"altNames"`
		Metadata []NameValueType `json:"metadata"`
	}

	//  RNASeqGex struct {
	// 	Dataset int     `json:"dataset"`
	// 	Sample  int     `json:"sample"`
	// 	Gene    int     `json:"gene"`
	// 	Counts  int     `json:"counts"`
	// 	TPM     float32 `json:"tpm"`
	// 	VST     float32 `json:"vst"`
	// }

	//  MicroarrayGex struct {
	// 	Dataset int     `json:"dataset"`
	// 	Sample  int     `json:"sample"`
	// 	Gene    int     `json:"gene"`
	// 	RMA     float32 `json:"vst"`
	// }

	ResultSample struct {
		//Dataset int     `json:"dataset"`
		Id string `json:"id"`
		//Gene    int     `json:"gene"`
		//Counts int     `json:"counts"`
		////TPM    float32 `json:"tpm"`
		//VST    float32 `json:"vst"`
		Value float32 `json:"value"`
	}

	ScrnaDB struct {
		db  *sql.DB
		dir string
	}
)

// approx size of dataset
const (
	DatasetSize = 500

	SpeciesSQL = `SELECT DISTINCT
		species,
		FROM datasets
		ORDER BY species`

	AssembliesSql = `SELECT
		datasets.assembly
		FROM datasets
		WHERE datasets.species = :species
		ORDER BY datasets.assembly`

	AllTechnologiesSql = `SELECT DISTINCT
		species, technology, platform 
		FROM datasets 
		ORDER BY species, technology, platform`

	// const ALL_VALUE_TYPES_SQL = `SELECT
	// 	gex_value_types.id,
	// 	gex_value_types.name
	// 	FROM gex_value_types
	// 	ORDER BY gex_value_types.platform_id, gex_value_types.id`

	// const VALUE_TYPES_SQL = `SELECT
	// 	gex_value_types.id,
	// 	gex_value_types.name
	// 	FROM gex_value_types
	// 	WHERE gex_value_types.platform_id = ?1
	// 	ORDER BY gex_value_types.id`

	DatasetsSql = `SELECT DISTINCT
		datasets.id,	
		permissions.name as permission,
		datasets.dataset_id,
		datasets.name,
		datasets.institution,
		datasets.species,
		datasets.assembly,
		datasets.cells,
		datasets.url,
		datasets.description
		FROM datasets
		JOIN dataset_permissions ON datasets.id = dataset_permissions.dataset_id
		JOIN permissions ON dataset_permissions.permission_id = permissions.id
		WHERE datasets.species = :species AND datasets.assembly = :assembly
		ORDER BY datasets.name`

	DatasetsPermissionsSql = `SELECT DISTINCT
		datasets.id,	
		permissions.name as permission,
		datasets.dataset_id
		FROM datasets
		JOIN dataset_permissions ON datasets.id = dataset_permissions.dataset_id
		JOIN permissions ON dataset_permissions.permission_id = permissions.id
		WHERE datasets.species = :species AND datasets.assembly = :assembly`

	DatasetPermissionsSql = `SELECT DISTINCT
		datasets.id,
		permissions.name as permission
		FROM datasets
		JOIN dataset_permissions ON datasets.id = dataset_permissions.dataset_id
		JOIN permissions ON dataset_permissions.permission_id = permissions.id
		WHERE datasets.dataset_id = :id`

	DatasetSql = `SELECT 
		datasets.id,
		datasets.dataset_id,
		datasets.name,
		datasets.institution,
		datasets.species,
		datasets.assembly,
		datasets.cells,
		datasets.url,
		datasets.description
		FROM datasets 
		WHERE datasets.dataset_id = :id`
)

// const DATASETS_SQL = `SELECT
// 	name
// 	FROM datasets
// 	ORDER BY datasets.name`

// type GexValue string

func NewScrnaDB(dir string) *ScrnaDB {

	// db, err := sql.Open("sqlite3", path)

	// if err != nil {
	// 	log.Fatal().Msgf("%s", err)
	// }

	// defer db.Close()

	return &ScrnaDB{dir: dir, db: sys.Must(sql.Open(sys.Sqlite3DB, filepath.Join(dir, "datasets.db")))}
}

func (sdb *ScrnaDB) Dir() string {
	return sdb.dir
}

func (sdb *ScrnaDB) Close() error {
	return sdb.db.Close()
}

// func (sdb *Datasetssdb) GetGenes(genes []string) ([]*GexGene, error) {
// 	db, err := sql.Open("sqlite3", sdb.dir)

// 	if err != nil {
// 		return nil, err
// 	}

// 	defer db.Close()

// 	ret := make([]*GexGene, 0, len(genes))

// 	for _, gene := range genes {
// 		var gexGene GexGene

// 		err := db.QueryRow(GENE_SQL, fmt.Sprintf("%%%s%%", gene)).Scan(&gexGene.Id, &gexGene.GeneId, &gexGene.GeneSymbol)

// 		if err == nil {
// 			ret = append(ret, &gexGene)
// 		}
// 	}

// 	return ret, nil
// }

func (sdb *ScrnaDB) Species() ([]string, error) {

	species := make([]string, 0, 10)

	rows, err := sdb.db.Query(SpeciesSQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var name string

		err := rows.Scan(
			&name)

		if err != nil {
			return nil, err
		}

		species = append(species, name)
	}

	return species, nil
}

func (sdb *ScrnaDB) Assemblies(species string) ([]string, error) {

	assemblies := make([]string, 0, 10)

	rows, err := sdb.db.Query(AssembliesSql, sql.Named("species", species))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var name string

		err := rows.Scan(
			&name)

		if err != nil {
			return nil, err
		}

		assemblies = append(assemblies, name)
	}

	return assemblies, nil
}

func (sdb *ScrnaDB) Datasets(species string, assembly string, permissions []string) ([]*Dataset, error) {

	// turn permissions into set
	permissionSet := sys.NewStringSet().ListUpdate(permissions)

	isAdmin := auth.HasAdminPermission(permissions)

	datasets := make([]*Dataset, 0, 10)

	log.Debug().Msgf("%s %s %v %v", species, assembly, permissionSet, isAdmin)

	datasetRows, err := sdb.db.Query(DatasetsSql, sql.Named("species", species), sql.Named("assembly", assembly))

	if err != nil {
		log.Debug().Msgf("%s", err)
		return nil, err
	}

	defer datasetRows.Close()

	var permission string
	var id string

	for datasetRows.Next() {
		var dataset Dataset

		err := datasetRows.Scan(
			&id,
			&permission,
			&dataset.Id,
			&dataset.Name,
			&dataset.Institution,
			&dataset.Species,
			&dataset.Assembly,
			&dataset.Cells,
			&dataset.Url,
			&dataset.Description)

		if err != nil {
			return nil, err
		}

		// we don't have permission for this dataset so skip
		if !isAdmin && !permissionSet.Has(permission) {
			continue
		}

		// log.Debug().Msgf("db %s", filepath.Join(sdb.dir, dataset.Url))

		// db2, err := sql.Open("sqlite3", filepath.Join(sdb.dir, dataset.Url))

		// if err != nil {
		// 	return nil, err
		// }

		// defer db2.Close()

		// err = db2.QueryRow(SAMPLE_COUNT_SQL, dataset.Id).Scan(&dataset.Cells)

		// if err != nil {
		// 	return nil, err
		// }

		datasets = append(datasets, &dataset)
	}

	return datasets, nil
}

// Return nil if user has permission to view dataset
func (sdb *ScrnaDB) HasPermissionToViewDataset(datasetId string, permissions []string) error {
	//return errors.New("not implemented")

	if auth.HasAdminPermission(permissions) {
		return nil
	}

	rows, err := sdb.db.Query(DatasetPermissionsSql, sql.Named("id", datasetId))

	if err != nil {
		log.Error().Msgf("checking dataset permissions %s", err)
		return err
	}

	defer rows.Close()

	permissionSet := sys.NewStringSet().ListUpdate(permissions)

	var id string
	var permission string

	for rows.Next() {
		err := rows.Scan(&id, &permission)

		if err != nil {
			log.Error().Msgf("scanning dataset permissions %s", err)
			return err
		}

		if permissionSet.Has(permission) {
			return nil
		}
	}

	return errors.New("not allowed to view dataset: " + datasetId)
}

func (sdb *ScrnaDB) dataset(datasetId string) (*Dataset, error) {

	var id string
	var dataset Dataset

	err := sdb.db.QueryRow(DatasetSql, sql.Named("id", datasetId)).Scan(
		&id,
		&dataset.Id,
		&dataset.Name,
		&dataset.Institution,
		&dataset.Species,
		&dataset.Assembly,
		&dataset.Cells,
		&dataset.Url,
		&dataset.Description)

	if err != nil {
		return nil, err
	}

	return &dataset, nil
}

func (sdb *ScrnaDB) Gex(datasetId string,
	geneIds []string) (*dat.GexResults, error) {

	dataset, err := sdb.dataset(datasetId)

	if err != nil {
		return nil, err
	}

	datasetsdb := NewDatasetDB(dataset)

	defer datasetsdb.Close()

	ret, err := datasetsdb.Gex(geneIds)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

// func (sdb *Datasetssdb) Metadata(publicId string) (*DatasetClusters, error) {

// 	dataset, err := sdb.dataset(publicId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	datasetsdb := NewDatasetsdb(dataset)

// 	ret, err := datasetsdb.Metadata()

// 	if err != nil {
// 		return nil, err
// 	}

// 	return ret, nil
// }

func (sdb *ScrnaDB) Metadata(id string) (*DatasetMetadata, error) {

	dataset, err := sdb.dataset(id)

	if err != nil {
		return nil, err
	}

	log.Debug().Msgf("Dataset id: %s", dataset.Id)

	datasetsdb := NewDatasetDB(dataset)

	ret, err := datasetsdb.Metadata()

	if err != nil {
		log.Error().Msgf("metadata %s", err)
		return nil, err
	}

	return ret, nil
}

func (sdb *ScrnaDB) Genes(id string) ([]*Gene, error) {

	dataset, err := sdb.dataset(id)

	if err != nil {
		return nil, err
	}

	datasetsdb := NewDatasetDB(dataset)

	ret, err := datasetsdb.Genes()

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (sdb *ScrnaDB) SearchGenes(datasetId string, query string, limit int16) ([]*Gene, error) {

	dataset, err := sdb.dataset(datasetId)

	if err != nil {
		return nil, err
	}

	datasetsdb := NewDatasetDB(dataset)

	ret, err := datasetsdb.SearchGenes(query, limit)

	if err != nil {
		return nil, err
	}

	return ret, nil
}
