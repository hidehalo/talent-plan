package main

type chunk struct {
	data   [][][]byte
	cursor int
}

func newChunk(cap int) *chunk {
	return &chunk{
		data:   make([][][]byte, 0, cap),
		cursor: 0,
	}
}

func (chunk *chunk) appendRow(row [][]byte) {
	if len(chunk.data) <= cap(chunk.data) {
		chunk.data = append(chunk.data, row)
	} else {
		chunk.data[chunk.cursor] = row
	}
	chunk.cursor++
}

func (chunk *chunk) reset() {
	chunk.data = chunk.data[:0]
	chunk.cursor = 0
}

func (chunk *chunk) isFull() bool {
	return chunk.cursor >= cap(chunk.data)
}

func (chunk *chunk) getData() [][][]byte {
	return chunk.data[:chunk.cursor]
}
