package scrna

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// keep them in the entered order so we can preserve
// groupings such as N/GC/M which are not alphabetical
const SAMPLES_SQL = `SELECT
	samples.id,
	samples.public_id,
	samples.name
	FROM samples
	ORDER BY samples.id`

const SAMPLE_ALT_NAMES_SQL = `SELECT
	sample_alt_names.name,
	sample_alt_names.value
	FROM sample_alt_names
	WHERE sample_alt_names.sample_id = ?1
	ORDER by sample_alt_names.id`

const SAMPLE_METADATA_SQL = `SELECT
	sample_metadata.name,
	sample_metadata.value
	FROM sample_metadata
	WHERE sample_metadata.sample_id = ?1
	ORDER by sample_metadata.id`

const GENE_SQL = `SELECT 
	genes.id, 
	genes.hugo_id,
	genes.mgi_id,
	genes.ensembl_id,
	genes.refseq_id,
	genes.gene_symbol 
	FROM genes
	WHERE genes.gene_symbol LIKE ?1 OR genes.hugo_id = ?1 OR genes.ensembl_id LIKE ?1 OR genes.refseq_id LIKE ?1 
	LIMIT 1`

const RNA_SQL = `SELECT
	expression.id,
	expression.counts,
	expression.tpm,
	expression.vst
	FROM expression 
	WHERE expression.gene_id = ?1`

type DatasetCache struct {
	dir     string
	dataset *Dataset
}

func NewDatasetCache(dir string, dataset *Dataset) *DatasetCache {
	return &DatasetCache{dir: dir, dataset: dataset}
}

func (cache *DatasetCache) FindGenes(genes []string) ([]*GexGene, error) {

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Url))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := make([]*GexGene, 0, len(genes))

	for _, g := range genes {
		var gene GexGene
		err := db.QueryRow(GENE_SQL, g).Scan(
			&gene.Id,
			&gene.Ensembl,
			&gene.GeneSymbol)

		if err == nil {
			// add as many genes as possible
			ret = append(ret, &gene)
		} else {
			// log that we couldn't find a gene, but continue
			// anyway
			log.Debug().Msgf("gene not found: %s", g)
			//return nil, err
		}
	}

	return ret, nil
}

func (cache *DatasetCache) FindGexValues(
	geneIds []string) (*SearchResults, error) {

	genes, err := cache.FindGenes(geneIds)

	if err != nil {
		return nil, err
	}

	//log.Debug().Msgf("cripes %v", filepath.Join(cache.dir, cache.dataset.Path))

	db, err := sql.Open("sqlite3", filepath.Join(cache.dir, cache.dataset.Url))

	if err != nil {
		return nil, err
	}

	defer db.Close()

	ret := SearchResults{

		Dataset:  cache.dataset.PublicId,
		Features: make([]*ResultFeature, 0, len(genes))}

	var id int
	var counts string
	var tpm string
	var vst string
	var gex string

	for _, gene := range genes {
		err := db.QueryRow(RNA_SQL, gene.Id).Scan(
			&id,
			&counts,
			&tpm,
			&vst)

		if err != nil {
			return nil, err
		}

		values := make([]float32, 0, DATASET_SIZE)

		for stringValue := range strings.SplitSeq(gex, ",") {
			value, err := strconv.ParseFloat(stringValue, 32)

			if err != nil {
				return nil, err
			}

			values = append(values, float32(value))
		}

		//log.Debug().Msgf("hmm %s %f %f", gexType, sample.Value, tpm)

		//datasetResults.Samples = append(datasetResults.Samples, &sample)
		ret.Features = append(ret.Features, &ResultFeature{Gene: gene, Expression: values})

	}

	return &ret, nil
}
