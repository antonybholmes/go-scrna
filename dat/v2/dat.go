package v2

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/antonybholmes/go-scrna/dat"
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

	//log.Debug().Msgf("Seeking to position: %s %d", file, seek)

	return _seekGexGeneFromDat(f, offset)
}

func _seekGexGeneFromDat(f *os.File, offset int64) (*dat.GexGene, error) {

	var total uint32
	binary.Read(io.NewSectionReader(f, offset, 4), binary.LittleEndian, &total)

	// Allocate buffer for whole record
	buf := make([]byte, total)
	_, err := f.ReadAt(buf, offset+4) // after total_length

	if err != nil {
		return nil, err
	}

	var record dat.GexGene

	cur := 0

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

	// values
	// skip reading number of values since decode will handle that
	//numValues := int(binary.LittleEndian.Uint32(buf[cur:]))
	cur += 4

	// Interpret remaining bytes as float32 slice
	//floats := make([]float32, numValues)
	//floatBytes := buf[cur:]
	//binary.Read(bytes.NewReader(floatBytes), binary.LittleEndian, &floats)

	// each entry is [cellIndex, expressionValue] so num is even and half
	// the number of entries
	values, err := DecodeFloat32Pairs(buf, cur)

	if err != nil {
		return nil, err
	}

	record.Data = values

	return &record, nil

}

func DecodeFloat32Pairs(buf []byte, offset int) ([][2]float32, error) {
	buf = buf[offset:] // start at the correct position

	if len(buf)%8 != 0 {
		return nil, fmt.Errorf("buffer length must be multiple of 8 bytes")
	}

	numPairs := len(buf) / 8 // each pair is 8 bytes (2x4byte float32)
	result := make([][2]float32, numPairs)

	off := 0
	for i := range numPairs {
		bits0 := binary.LittleEndian.Uint32(buf[off:])
		bits1 := binary.LittleEndian.Uint32(buf[off+4:])
		result[i] = [2]float32{
			math.Float32frombits(bits0),
			math.Float32frombits(bits1),
		}

		off += 8
	}

	return result, nil
}
