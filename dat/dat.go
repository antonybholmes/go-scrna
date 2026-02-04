package dat

type (
	// Used only for reading data
	GexGene struct {
		GeneId     string `json:"geneId" msgpack:"id"`
		GeneSymbol string `json:"geneSymbol" msgpack:"s"`
		// msgpack forced encoding of 32bit floats as that
		// is sufficient precision for gene expression data
		// Each entry is [cellIndex, expressionValue] to save space
		// since we only record non-zero values
		// Cell index is uint32 but we store as float32 for msgpack
		// compatibility
		//Data       [][2]float32 `json:"gex" msgpack:"d"`
		Indexes []uint32  `json:"indexes" msgpack:"i"`
		Gex     []float32 `json:"gex" msgpack:"g"`
	}

	// ResultDataset struct {
	// 	Id string `json:"id"`
	// 	//Values   []float32 `json:"values"`
	// }

	GexResults struct {
		// we use the simpler value type for platform in search
		// results so that the value types are not repeated in
		// each search. The useful info in a search is just
		// the platform name and id

		//Dataset *Dataset      `json:"dataset"`
		Dataset string     `json:"dataset"`
		Genes   []*GexGene `json:"genes"`
	}
)
