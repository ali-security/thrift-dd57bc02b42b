/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bytes"
	"compress/zlib"
	"context"
	"testing"
)

func TestZlibTransport(t *testing.T) {
	trans, err := NewTZlibTransport(NewTMemoryBuffer(), zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

type DummyTransportFactory struct{}

func (p *DummyTransportFactory) GetTransport(trans TTransport) (TTransport, error) {
	return NewTMemoryBuffer(), nil
}

func TestZlibFactoryTransportWithFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(
		zlib.BestCompression,
		&DummyTransportFactory{},
	)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

func TestZlibFactoryTransportWithoutFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(zlib.BestCompression, nil)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

func TestZlibTransportMessageSizeLimit(t *testing.T) {
	const dataSize = 4096
	data := bytes.Repeat([]byte{'a'}, dataSize)

	// Write and flush (not close) so TMemoryBuffer.Close() doesn't wipe the data.
	writeBuf := NewTMemoryBuffer()
	writer, err := NewTZlibTransport(writeBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	compressed := make([]byte, writeBuf.Len())
	copy(compressed, writeBuf.Bytes())

	readBuf := NewTMemoryBuffer()
	readBuf.Write(compressed)
	reader, err := NewTZlibTransport(readBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetTConfiguration(&TConfiguration{MaxMessageSize: 1024})

	_, err = reader.Read(make([]byte, dataSize))
	if err == nil {
		t.Fatal("expected SIZE_LIMIT error, got nil")
	}
	protoEx, ok := err.(TProtocolException)
	if !ok || protoEx.TypeId() != SIZE_LIMIT {
		t.Fatalf("expected SIZE_LIMIT TProtocolException, got %T: %v", err, err)
	}
}

// TestZlibTransportDecompressionBombBounded checks that the decompressed size
// is accounted for across the whole reader, not per Read call, so a small
// compressed payload cannot be amplified into an unbounded amount of data by
// reading it in small chunks.
func TestZlibTransportDecompressionBombBounded(t *testing.T) {
	const bombSize = 8 << 20 // 8MiB of highly compressible data
	const maxMessageSize = 64 * 1024
	const chunkSize = 4096

	// Write and flush (not close) so TMemoryBuffer.Close() doesn't wipe the data.
	writeBuf := NewTMemoryBuffer()
	writer, err := NewTZlibTransport(writeBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte{0}, bombSize)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	compressed := make([]byte, writeBuf.Len())
	copy(compressed, writeBuf.Bytes())
	if len(compressed) >= maxMessageSize {
		t.Fatalf(
			"expected the payload to be an amplification (compressed %d bytes, limit %d)",
			len(compressed),
			maxMessageSize,
		)
	}

	readBuf := NewTMemoryBuffer()
	readBuf.Write(compressed)
	reader, err := NewTZlibTransport(readBuf, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetTConfiguration(&TConfiguration{MaxMessageSize: maxMessageSize})

	// Read in chunks well under the limit: without cumulative accounting every
	// single Read looks harmless and the full 8MiB gets decompressed.
	chunk := make([]byte, chunkSize)
	var total int64
	var readErr error
	for total < bombSize {
		n, err := reader.Read(chunk)
		total += int64(n)
		if err != nil {
			readErr = err
			break
		}
	}
	if readErr == nil {
		t.Fatalf("expected the read to be aborted, but decompressed %d bytes", total)
	}
	protoEx, ok := readErr.(TProtocolException)
	if !ok || protoEx.TypeId() != SIZE_LIMIT {
		t.Fatalf("expected SIZE_LIMIT TProtocolException, got %T: %v", readErr, readErr)
	}
	if maxTotal := int64(maxMessageSize + chunkSize); total > maxTotal {
		t.Errorf("decompressed %d bytes, want no more than %d", total, maxTotal)
	}
}
