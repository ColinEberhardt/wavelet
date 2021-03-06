// Copyright (c) 2019 Perlin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package store

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkLevelDB(b *testing.B) {
	path := "level"
	_ = os.RemoveAll(path)

	b.StopTimer()

	db, err := NewLevelDB(path)
	assert.NoError(b, err)
	defer os.RemoveAll(path)
	defer db.Close()

	b.StartTimer()
	defer b.StopTimer()

	for i := 0; i < b.N; i++ {
		var randomKey [128]byte
		var randomValue [600]byte

		_, err := rand.Read(randomKey[:])
		assert.NoError(b, err)
		_, err = rand.Read(randomValue[:])
		assert.NoError(b, err)

		err = db.Put(randomKey[:], randomValue[:])
		assert.NoError(b, err)

		value, err := db.Get(randomKey[:])
		assert.NoError(b, err)

		assert.EqualValues(b, randomValue[:], value)
	}
}

func TestLevelDBExistence(t *testing.T) {
	path := "level"
	_ = os.RemoveAll(path)

	db, err := NewLevelDB(path)
	assert.NoError(t, err)
	defer os.RemoveAll(path)
	defer db.Close()

	_, err = db.Get([]byte("not_exist"))
	assert.Error(t, err)

	err = db.Put([]byte("exist"), []byte{})
	assert.NoError(t, err)

	val, err := db.Get([]byte("exist"))
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, val)
}

func TestLevelDB(t *testing.T) {
	path := "level"
	_ = os.RemoveAll(path)

	db, err := NewLevelDB(path)
	assert.NoError(t, err)
	defer os.RemoveAll(path)

	err = db.Put([]byte("exist"), []byte("value"))
	assert.NoError(t, err)

	wb := db.NewWriteBatch()
	wb.Put([]byte("key_batch1"), []byte("val_batch1"))
	wb.Put([]byte("key_batch2"), []byte("val_batch2"))
	wb.Put([]byte("key_batch3"), []byte("val_batch2"))
	assert.NoError(t, db.CommitWriteBatch(wb))

	assert.NoError(t, db.Close())

	db2, err := NewLevelDB(path)
	assert.NoError(t, err)

	v, err := db2.Get([]byte("exist"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), v)

	// Check multiget
	mv, err := db2.MultiGet([]byte("key_batch1"), []byte("key_batch2"))
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("val_batch1"), []byte("val_batch2")}, mv)

	// Check delete
	assert.NoError(t, db2.Delete([]byte("exist")))

	_, err = db2.Get([]byte("exist"))
	assert.Error(t, err)
}
