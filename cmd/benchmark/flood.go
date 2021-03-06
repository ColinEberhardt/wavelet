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

package main

import (
	"runtime"
	"sync"

	"github.com/perlin-network/wavelet"
	"github.com/perlin-network/wavelet/sys"
	"github.com/perlin-network/wavelet/wctl"
	"github.com/pkg/errors"
)

func floodTransactions() func(client *wctl.Client) ([]wctl.SendTransactionResponse, error) {
	return func(client *wctl.Client) ([]wctl.SendTransactionResponse, error) {
		numWorkers := runtime.NumCPU()

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		chRes := make(chan wctl.SendTransactionResponse, numWorkers)
		chErr := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			go sendTransaction(i+1, client, &wg, chRes, chErr)
		}

		wg.Wait()

		var responses []wctl.SendTransactionResponse
		var err error

		for i := 0; i < numWorkers; i++ {
			if e := <-chErr; e != nil {
				if err == nil {
					err = e
				} else {
					err = errors.Wrap(err, e.Error())
				}
			}

			responses = append(responses, <-chRes)
		}

		return responses, err
	}
}

func sendTransaction(
	i int,
	client *wctl.Client,
	wg *sync.WaitGroup,
	chRes chan<- wctl.SendTransactionResponse,
	chErr chan<- error) {

	defer wg.Done()

	n := 40
	payload := wavelet.Batch{
		Tags:     make([]uint8, 0, n),
		Payloads: make([][]byte, 0, n),
	}

	stake := wavelet.Stake{Opcode: sys.PlaceStake, Amount: uint64(i)}
	for i := 0; i < 40; i++ {
		if err := payload.AddStake(stake); err != nil {
			// Shouldn't happen
			panic(err)
		}
	}

	var res wctl.SendTransactionResponse
	res, err := client.SendTransaction(byte(sys.TagBatch), payload.Marshal())
	if err != nil {
		chRes <- res
		chErr <- err
		return
	}

	chRes <- res
	chErr <- err
}
