package dat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

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

func SeekGexGeneFromDat(file string, offset int64) (*GexGene, error) {
	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	//log.Debug().Msgf("Seeking to position: %s %d", file, offset)

	var blockSize uint32
	binary.Read(io.NewSectionReader(f, offset, 4), binary.LittleEndian, &blockSize)

	// block size includes the 4 bytes of the block size itself, so subtract that to get the size of the actual data
	length := blockSize - 4

	// Allocate buffer for whole record
	buf := make([]byte, length)
	_, err = io.ReadFull(io.NewSectionReader(f, offset+4, int64(length)), buf) // after total_length

	if err != nil {
		return nil, err
	}

	var record GexGene
	var cur int = 0

	//log.Debug().Msgf("Read block size: %d for record at offset %d", blockSize, offset)

	cur, err = extractGeneName(buf, cur, &record)

	if err != nil {
		return nil, err
	}

	//log.Debug().Msgf("Read gene: %s (%s) with total record size %d", record.GeneSymbol, record.GeneId, blockSize)

	// values
	// skip reading number of values since decode will handle that
	//size := int(binary.LittleEndian.Uint32(buf[cur:]))
	cur += 4

	//record.Indexes = make([]uint32, size)
	//record.Gex = make([]float32, size)

	// Interpret remaining bytes as float32 slice
	//floats := make([]float32, numValues)
	//floatBytes := buf[cur:]
	//binary.Read(bytes.NewReader(floatBytes), binary.LittleEndian, &floats)

	// each entry is [cellIndex, expressionValue] so num is even and half
	// the number of entries
	err = decodeFloat32Pairs(buf, cur, &record)

	if err != nil {
		return nil, err
	}

	return &record, nil
}

func extractGeneName(buf []byte, cur int, record *GexGene) (int, error) {
	if len(buf) < cur+2 {
		return cur, errors.New("not enough data for GeneId length")
	}

	// Extract GeneId
	geneIdLen := int(binary.LittleEndian.Uint16(buf[cur : cur+2]))
	cur += 2

	if len(buf) < cur+geneIdLen {
		return cur, errors.New("not enough data for GeneId string")
	}

	record.GeneId = string(buf[cur : cur+geneIdLen])
	cur += geneIdLen

	// Extract GeneSymbol
	if len(buf) < cur+2 {
		return cur, errors.New("not enough data for GeneSymbol length")
	}

	geneSymbolLen := int(binary.LittleEndian.Uint16(buf[cur : cur+2]))
	cur += 2

	if len(buf) < cur+geneSymbolLen {
		return cur, errors.New("not enough data for GeneSymbol string")
	}
	record.GeneSymbol = string(buf[cur : cur+geneSymbolLen])
	cur += geneSymbolLen

	return cur, nil
}

func decodeFloat32Pairs(buf []byte, offset int, record *GexGene) error {
	//log.Debug().Msgf("Decoding float32 pairs: offset=%d size=%d bufLen=%d", offset, size, len(buf))

	buf = buf[offset:] // : offset+size] // start at the correct position

	//log.Debug().Msgf("len(buf)=%d", len(buf))

	if len(buf)%8 != 0 {
		return fmt.Errorf("buffer length must be multiple of 8 bytes")
	}

	numPairs := len(buf) / 8 // each pair is 8 bytes (2x4byte float32)
	//positions := make([]int32, numPairs)
	//expression := make([]float32, numPairs)

	record.Indexes = make([]uint32, numPairs)
	record.Gex = make([]float32, numPairs)

	readBuf := bytes.NewReader(buf)

	//byteLen := int64(numPairs * 4)

	// Read the data into the slices since they are contiguous in memory
	// we can read directly into them one after the other without using
	// NewSectionReader etc
	binary.Read(readBuf, binary.LittleEndian, &record.Indexes)
	binary.Read(readBuf, binary.LittleEndian, &record.Gex)

	// // Combine into [][2]float32
	// result := make([][2]float32, numPairs)

	// off := 0
	// for i := range numPairs {
	// 	indexBits := binary.LittleEndian.Uint32(buf[off:])
	// 	expBits := binary.LittleEndian.Uint32(buf[off+4:])
	// 	result[i] = [2]float32{
	// 		math.Float32frombits(indexBits),
	// 		math.Float32frombits(expBits),
	// 	}

	// 	off += 8
	// }

	return nil
}
