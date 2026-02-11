package v2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/antonybholmes/go-scrna/dat"
	"github.com/antonybholmes/go-sys/log"
)

// func ReadGexGeneFromDat(file string, index int) (*dat.GexGene, error) {
// 	f, err := os.Open(file)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer f.Close()

// 	// Read offset table (256 uint32s = 1024 bytes)
// 	// skip magic   + version + num cells = 4 + 4 + 4 = 12 bytes
// 	f.Seek(12, 0) // Skip the magic byte, version, and num cells
// 	var numEntries int32
// 	err = binary.Read(f, binary.LittleEndian, &numEntries)
// 	if err != nil {
// 		return nil, err
// 	}

// 	offsets := make([]int32, numEntries*2)
// 	err = binary.Read(f, binary.LittleEndian, &offsets)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if index < 0 || index >= len(offsets) {
// 		return nil, fmt.Errorf("index out of range")
// 	}

// 	// Calculate absolute position of the record in the file
// 	// Header size: (magic, always 42) + version + num cells + 4 (numEntries) + numEntries*4*2 (offsets)
// 	// each entry is a 4byte offset and a 4byte size, hence 4*2
// 	dataStart := int64(16 + numEntries*8) // header size
// 	recordOffset := int64(offsets[index])
// 	recordSize := offsets[index+1]
// 	recordPos := dataStart + recordOffset

// 	return _seekGexGeneFromDat(f, recordPos, recordSize)

// }

func SeekGexGeneFromDat(file string, offset int64) (*dat.GexGene, error) {
	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	log.Debug().Msgf("Seeking to position: %s %d", file, offset)

	var blockSize uint32
	binary.Read(io.NewSectionReader(f, offset, 4), binary.LittleEndian, &blockSize)

	// Allocate buffer for whole record
	buf := make([]byte, blockSize)
	_, err = f.ReadAt(buf, offset) // after total_length

	if err != nil {
		return nil, err
	}

	var record dat.GexGene

	cur := 4 // skip total_length since we already have it

	// key1
	geneIdLen := int(binary.LittleEndian.Uint16(buf[cur:]))
	cur += 2
	record.GeneId = string(buf[cur : cur+geneIdLen])
	cur += geneIdLen

	// key2
	geneSymbolLen := int(binary.LittleEndian.Uint16(buf[cur:]))
	cur += 2
	record.GeneSymbol = string(buf[cur : cur+geneSymbolLen])
	cur += geneSymbolLen

	log.Debug().Msgf("Read gene: %s (%s) with total record size %d", record.GeneSymbol, record.GeneId, blockSize)

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

func decodeFloat32Pairs(buf []byte, offset int, record *dat.GexGene) error {
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
