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

package wavelet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/perlin-network/wavelet/log"
	"github.com/perlin-network/wavelet/sys"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

type Protocol struct {
	ledger *Ledger
}

func (p *Protocol) Gossip(stream Wavelet_GossipServer) error {
	for {
		batch, err := stream.Recv()

		if err != nil {
			return err
		}

		for _, buf := range batch.Transactions {
			tx, err := UnmarshalTransaction(bytes.NewReader(buf))

			if err != nil {
				logger := log.TX("gossip")
				logger.Err(err).Msg("Failed to unmarshal transaction")
				continue
			}

			if err := p.ledger.AddTransaction(tx); err != nil && errors.Cause(err) != ErrMissingParents {
				fmt.Printf("error adding incoming tx to graph [%v]: %+v\n", err, tx)
			}
		}
	}
}

func (p *Protocol) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	res := &QueryResponse{}

	round, err := p.ledger.rounds.GetByIndex(req.RoundIndex)
	if err == nil {
		res.Round = round.Marshal()
		return res, nil
	}

	preferred := p.ledger.finalizer.Preferred()
	if preferred != nil {
		res.Round = preferred.(*Round).Marshal()
		return res, nil
	}

	return res, nil
}

// channelBuffer is a buffer which blocks write if the
// buffer is full.
type channelBuffer struct {
	Bytes chan byte

	// If reading blocks longer than ReadTimeout,
	// io.EOF is returned.
	ReadTimeout time.Duration
}

func (b *channelBuffer) Read(buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		select {
		case bb := <-b.Bytes:
			buf[n] = bb
			n++

		case <-time.After(b.ReadTimeout):
			return n, io.EOF
		}
	}

	return n, nil
}

func (b *channelBuffer) Write(buf []byte) (int, error) {
	for _, bb := range buf {
		// Write will block indefinitely, basically slowing
		// down diff dumping if read is too fast
		b.Bytes <- bb
	}

	return len(buf), nil
}

func (p *Protocol) Sync(stream Wavelet_SyncServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	res := &SyncResponse{}

	// The diffs are dumped into an in-memory buffer, while at the same time,
	// it is chunked as fast as possible. This avoids needing to write the chunks
	// into memory, as that is being handled by cacheChunks.
	buf := &channelBuffer{Bytes: make(chan byte, 1024), ReadTimeout: time.Millisecond * 100}
	header := &SyncInfo{LatestRound: p.ledger.rounds.Latest().Marshal()}

	chunking := make(chan struct{})
	go func() {
		defer close(chunking)

		var chunk [sys.SyncChunkSize]byte
		for {
			n, err := buf.Read(chunk[:])
			if n > 0 {
				checksum := blake2b.Sum256(chunk[:n])
				p.ledger.chunks.Put(checksum, chunk[:n])

				header.Checksums = append(header.Checksums, checksum[:])
			}

			if err == io.EOF {
				return
			}
		}
	}()

	if err := p.ledger.accounts.Snapshot().DumpDiff(req.GetRoundId(), buf); err != nil {
		return err
	}

	// Wait for chunking to finish
	<-chunking

	res.Data = &SyncResponse_Header{Header: header}

	if err := stream.Send(res); err != nil {
		return err
	}

	res.Data = &SyncResponse_Chunk{}

	for {
		req, err := stream.Recv()

		if err != nil {
			return err
		}

		var checksum [blake2b.Size256]byte
		copy(checksum[:], req.GetChecksum())

		chunk, err := p.ledger.chunks.Get(checksum)
		if err != nil {
			return err
		}

		logger := log.Sync("provide_chunk")
		logger.Info().
			Hex("requested_hash", req.GetChecksum()).
			Msg("Responded to sync chunk request.")

		res.Data.(*SyncResponse_Chunk).Chunk = chunk

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (p *Protocol) CheckOutOfSync(ctx context.Context, req *OutOfSyncRequest) (*OutOfSyncResponse, error) {
	return &OutOfSyncResponse{
		OutOfSync: p.ledger.rounds.Latest().Index >= sys.SyncIfRoundsDifferBy+req.RoundIndex,
	}, nil
}

func (p *Protocol) DownloadMissingTx(ctx context.Context, req *DownloadMissingTxRequest) (*DownloadTxResponse, error) {
	res := &DownloadTxResponse{Transactions: make([][]byte, 0, len(req.Ids))}

	for _, buf := range req.Ids {
		var id TransactionID
		copy(id[:], buf)

		if tx := p.ledger.Graph().FindTransaction(id); tx != nil {
			res.Transactions = append(res.Transactions, tx.Marshal())
		}
	}

	return res, nil
}

func (p *Protocol) DownloadTx(ctx context.Context, req *DownloadTxRequest) (*DownloadTxResponse, error) {
	lowLimit := req.Depth - sys.MaxDepthDiff
	highLimit := req.Depth + sys.MaxDownloadDepthDiff

	receivedIDs := make(map[TransactionID]struct{}, len(req.SkipIds))
	for _, buf := range req.SkipIds {
		var id TransactionID
		copy(id[:], buf)

		receivedIDs[id] = struct{}{}
	}

	var txs [][]byte
	hostTXs := p.ledger.Graph().GetTransactionsByDepth(&lowLimit, &highLimit)
	for _, tx := range hostTXs {
		if _, ok := receivedIDs[tx.ID]; !ok {
			txs = append(txs, tx.Marshal())
		}
	}

	return &DownloadTxResponse{Transactions: txs}, nil
}
