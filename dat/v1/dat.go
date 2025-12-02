package v1

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/antonybholmes/go-scrna/dat"
	"github.com/vmihailenco/msgpack/v5"
)

func ReadGexGeneFromDat(file string, index int) (*dat.GexGene, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read offset table (256 uint32s = 1024 bytes)
	// skip magic   + version + num cells = 4 + 4 + 4 = 12 bytes
	f.Seek(12, 0) // Skip the magic byte, version, and num cells
	var numEntries int32
	err = binary.Read(f, binary.LittleEndian, &numEntries)
	if err != nil {
		return nil, err
	}

	offsets := make([]int32, numEntries*2)
	err = binary.Read(f, binary.LittleEndian, &offsets)
	if err != nil {
		return nil, err
	}

	if index < 0 || index >= len(offsets) {
		return nil, fmt.Errorf("index out of range")
	}

	// Calculate absolute position of the record in the file
	// Header size: (magic, always 42) + version + num cells + 4 (numEntries) + numEntries*4*2 (offsets)
	// each entry is a 4byte offset and a 4byte size, hence 4*2
	dataStart := int64(16 + numEntries*8) // header size
	recordOffset := int64(offsets[index])
	recordSize := offsets[index+1]
	recordPos := dataStart + recordOffset

	return _seekGexGeneFromDat(f, recordPos, recordSize)

}

func SeekGexGeneFromDat(file string, seek int64, size int32) (*dat.GexGene, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	//log.Debug().Msgf("Seeking to position: %s %d", file, seek)

	return _seekGexGeneFromDat(f, seek, size)
}

func _seekGexGeneFromDat(f *os.File, seek int64, size int32) (*dat.GexGene, error) {

	// Read offset table (256 uint32s = 1024 bytes)
	_, err := f.Seek(seek, 0) // Skip the magic byte

	if err != nil {
		return nil, err
	}

	// Read exactly 'size' bytes
	buf := make([]byte, size)
	_, err = io.ReadFull(f, buf)

	if err != nil {
		return nil, err
	}

	// Use MessagePack decoder from current position
	//dec := msgpack.NewDecoder(f)

	var record dat.GexGene

	//err := dec.Decode(&record)

	err = msgpack.Unmarshal(buf, &record)

	if err != nil {
		return nil, err
	}

	return &record, nil
}
