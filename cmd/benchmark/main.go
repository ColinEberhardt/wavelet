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
	"encoding/hex"
	"fmt"
	"github.com/perlin-network/noise/edwards25519"
	"github.com/perlin-network/noise/skademlia"
	logger "github.com/perlin-network/wavelet/log"
	"github.com/perlin-network/wavelet/sys"
	"github.com/perlin-network/wavelet/wctl"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fastjson"
	"gopkg.in/urfave/cli.v1"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	app := cli.NewApp()

	app.Name = "benchmark"
	app.Author = "Perlin"
	app.Email = "support@perlin.net"
	app.Version = sys.Version
	app.Usage = "a benchmarking tool for wavelet nodes"

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("Version:    %s\n", sys.Version)
		fmt.Printf("Go Version: %s\n", sys.GoVersion)
		fmt.Printf("Git Commit: %s\n", sys.GitCommit)
		fmt.Printf("OS/Arch:    %s\n", sys.OSArch)
		fmt.Printf("Built:      %s\n", c.App.Compiled.Format(time.ANSIC))
	}

	app.Before = func(context *cli.Context) error {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Output(logger.NewConsoleWriter(nil))

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:  "local",
			Usage: "spawn some number of nodes locally and spam transactions on all of them",
			Flags: []cli.Flag{
				cli.UintFlag{
					Name:  "count",
					Usage: "number of nodes to spawn",
					Value: 2,
				},
			},
			Action: commandLocal,
		},
		{
			Name:  "remote",
			Usage: "connect to an already-running node and spam transactions on it",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "host",
					Usage: "node HTTP API address",
					Value: "127.0.0.1:9000",
				},
				cli.StringFlag{
					Name:  "wallet",
					Usage: "private key in hex format to connect to node HTTP API with",
					Value: "87a6813c3b4cf534b6ae82db9b1409fa7dbd5c13dba5858970b56084c4a930eb400056ee68a7cc2695222df05ea76875bc27ec6e61e8e62317c336157019c405",
				},
			},
			Action: commandRemote,
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("failed to parse configuration/command-line arguments: %+v\n", err)
	}
}

func commandRemote(c *cli.Context) error {
	args := strings.Split(c.String("host"), ":")

	if len(args) != 2 || len(args[0]) == 0 || len(args[1]) == 0 {
		return errors.New("host and port must be specified [example: 127.0.0.1:3000]")
	}

	host := args[0]

	var port uint16

	if p, err := strconv.ParseUint(args[1], 10, 16); err != nil {
		return errors.Wrap(err, "failed to decode port")
	} else {
		port = uint16(p)
	}

	wallet := c.String("wallet")

	var k *skademlia.Keypair

	// If a private key is specified instead of a path to a wallet, then simply use the provided private key instead.

	privateKeyBuf, err := ioutil.ReadFile(wallet)

	if err != nil && os.IsNotExist(err) && len(wallet) == hex.EncodedLen(edwards25519.SizePrivateKey) {
		var privateKey edwards25519.PrivateKey

		n, err := hex.Decode(privateKey[:], []byte(wallet))
		if err != nil {
			log.Fatal().Err(err).Msgf("Failed to decode the private key specified: %s", wallet)
		}

		if n != edwards25519.SizePrivateKey {
			log.Fatal().Msgf("Private key %s is not of the right length.", wallet)
			return nil
		}

		k, err = skademlia.LoadKeys(privateKey, sys.SKademliaC1, sys.SKademliaC2)
		if err != nil {
			log.Fatal().Err(err).Msgf("The private key specified is invalid: %s", wallet)
			return nil
		}

		privateKey, publicKey := k.PrivateKey(), k.PublicKey()

		log.Info().
			Hex("privateKey", privateKey[:]).
			Hex("publicKey", publicKey[:]).
			Msg("Loaded wallet.")
	} else if err != nil && os.IsNotExist(err) {
		log.Fatal().Msgf("Could not find an existing wallet at %q.", wallet)
		return nil
	} else if err != nil {
		log.Warn().Err(err).Msgf("Encountered an unexpected error loading your wallet from %q.", wallet)
		return nil
	} else {
		var privateKey edwards25519.PrivateKey

		n, err := hex.Decode(privateKey[:], privateKeyBuf)
		if err != nil {
			log.Fatal().Err(err).Msgf("Failed to decode your private key from %q.", wallet)
			return nil
		}

		if n != edwards25519.SizePrivateKey {
			log.Fatal().Msgf("Private key located in %q is not of the right length.", wallet)
			return nil
		}

		k, err = skademlia.LoadKeys(privateKey, sys.SKademliaC1, sys.SKademliaC2)
		if err != nil {
			log.Fatal().Err(err).Msgf("The private key specified in %q is invalid.", wallet)
			return nil
		}

		privateKey, publicKey := k.PrivateKey(), k.PublicKey()

		log.Info().
			Hex("privateKey", privateKey[:]).
			Hex("publicKey", publicKey[:]).
			Msg("Loaded wallet.")
	}

	client, err := connectToAPI(host, port, k.PrivateKey())
	if err != nil {
		return errors.Wrap(err, "failed to connect to node HTTP API")
	}

	fmt.Println("You're now connected!")

	go func() {
		events, err := client.PollLoggerSink(nil, wctl.RouteWSMetrics)
		if err != nil {
			panic(err)
		}

		var p fastjson.Parser

		for evt := range events {
			v, err := p.ParseBytes(evt)

			if err != nil {
				continue
			}

			log.Info().
				Float64("accepted_tps", v.GetFloat64("tps.accepted")).
				Float64("received_tps", v.GetFloat64("tps.received")).
				Float64("gossiped_tps", v.GetFloat64("tps.gossiped")).
				Float64("downloaded_tps", v.GetFloat64("tps.downloaded")).
				Float64("queried_rps", v.GetFloat64("rps.queried")).
				Int64("query_latency_max_ms", v.GetInt64("query.latency.max.ms")).
				Int64("query_latency_min_ms", v.GetInt64("query.latency.min.ms")).
				Float64("query_latency_mean_ms", v.GetFloat64("query.latency.mean.ms")).
				Msg("Benchmarking...")
		}
	}()

	flood := floodTransactions()

	for {
		if _, err := flood(client); err != nil {
			continue
		}
	}
}

func commandLocal(c *cli.Context) error {
	build()

	count := c.Uint("count")

	if count == 0 {
		return errors.New("count must be > 0")
	}

	nodes := []*node{
		spawn(nextAvailablePort(), nextAvailablePort(), false),
	}

	for i := uint(0); i < count-1; i++ {
		nodes = append(nodes, spawn(nextAvailablePort(), nextAvailablePort(), true, fmt.Sprintf("127.0.0.1:%d", nodes[0].nodePort)))
	}

	wait(nodes...)

	fmt.Println("Nodes are initialized!")

	flood := floodTransactions()

	for {
		if _, err := flood(nodes[0].client); err != nil {
			fmt.Println(err)
		}
	}
}
