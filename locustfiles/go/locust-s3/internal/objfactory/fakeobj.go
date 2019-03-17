/*
Copyright 2019 TWO SIGMA OPEN SOURCE, LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package objfactory

import (
	"errors"
	"io"
	"math/rand"
)

const bufferSizeBits = 23
const bufferSize = 1 << bufferSizeBits // a shared buffer to reduce run time overhead

var bufferBytes []byte

func init() {
	const bufferSize = 1 << bufferSizeBits
	bufferBytes = make([]byte, bufferSize, bufferSize)
	if _, err := rand.Read(bufferBytes); err != nil {
		panic("could not initiate buffer")
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// FakeObjReadSeeker implement a io.ReadSeeker interface to expose an arbitary size buffer
func FakeObjReadSeeker(size int64) io.ReadSeeker {
	return &fakeObjReader{size, 0}
}

type fakeObjReader struct {
	size   int64 // the size of the object this reader represent
	curPos int64 // current reading index
}

func (r *fakeObjReader) Read(b []byte) (n int, err error) {
	if r.curPos >= r.size {
		return 0, io.EOF
	}
	// TODO, need a better bitmask based way
	posInBuffer := r.curPos - (r.curPos>>bufferSizeBits)<<bufferSizeBits
	n = copy(b, bufferBytes[posInBuffer:min(posInBuffer+(r.size-r.curPos), bufferSize)])
	r.curPos += int64(n)
	return
}

func (r *fakeObjReader) Seek(offset int64, whence int) (int64, error) {
	// similar to https://golang.org/src/bytes/reader.go?s=2903:2965#L107
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.curPos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("bytes.Reader.Seek: negative position")
	}
	r.curPos = abs
	return abs, nil
}
