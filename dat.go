package scrna

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

func ReadRecordFromDat(file string, index int) (interface{}, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read offset table (256 uint32s = 1024 bytes)
	f.Seek(1, 0) // Skip the magic byte
	var numEntries uint32
	err = binary.Read(f, binary.LittleEndian, &numEntries)
	if err != nil {
		return nil, err
	}

	offsets := make([]uint32, numEntries)
	err = binary.Read(f, binary.LittleEndian, &offsets)
	if err != nil {
		return nil, err
	}

	if index < 0 || index >= len(offsets) {
		return nil, fmt.Errorf("index out of range")
	}

	// Calculate absolute position of the record in the file
	dataStart := int64(1 + 4 + numEntries*4) // header size
	recordOffset := int64(offsets[index])
	recordPos := dataStart + recordOffset

	return _seekRecordFromDat(f, recordPos)

}

func SeekRecordFromDat(file string, seek int64) (*GexGene, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	//log.Debug().Msgf("Seeking to position: %s %d", file, seek)

	return _seekRecordFromDat(f, seek)
}

func _seekRecordFromDat(f *os.File, seek int64) (*GexGene, error) {

	// Read offset table (256 uint32s = 1024 bytes)
	f.Seek(seek, 0) // Skip the magic byte

	// Use MessagePack decoder from current position
	dec := msgpack.NewDecoder(f)

	var record GexGene

	err := dec.Decode(&record)
	if err != nil {
		return nil, err
	}

	return &record, nil
}
