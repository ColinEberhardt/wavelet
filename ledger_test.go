package wavelet

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/perlin-network/wavelet/sys"
	"github.com/stretchr/testify/assert"
)

// TestLedger_BroadcastNop checks that:
//
// * The ledger will keep broadcasting nop tx as long
//   as there are unapplied tx (latestTxDepth <= rootDepth).
//
// * The ledger will stop broadcasting nop once there
//   are no more unapplied tx.
func TestLedger_BroadcastNop(t *testing.T) {
	testnet := NewTestNetwork(t)
	defer testnet.Cleanup()

	alice := testnet.AddNode(t)
	bob := testnet.AddNode(t)

	assert.True(t, <-alice.WaitForSync())
	assert.True(t, <-bob.WaitForSync())

	_, err := testnet.faucet.Pay(alice, 1000000)
	assert.NoError(t, err)

	// Wait for alice to receive her PERL from the faucet
	for range alice.WaitForConsensus() {
		if alice.Balance() > 0 {
			break
		}
	}

	// Add lots of transactions
	var txsLock sync.Mutex
	txs := make([]Transaction, 0, 10000)

	go func() {
		for i := 0; i < cap(txs); i++ {
			tx, err := alice.Pay(bob, 1)
			assert.NoError(t, err)

			txsLock.Lock()
			txs = append(txs, tx)
			txsLock.Unlock()

			// Somehow this prevents AddTransaction from
			// returning ErrMissingParents
			time.Sleep(time.Nanosecond * 1)
		}
	}()

	prevRound := alice.ledger.Rounds().Latest().Index
	timeout := time.NewTimer(time.Minute * 5)
	for {
		select {
		case <-timeout.C:
			t.Fatal("timed out before all transactions are applied")

		case <-alice.WaitForConsensus():
			var appliedCount int
			var txsCount int

			txsLock.Lock()
			for _, tx := range txs {
				if alice.Applied(tx) {
					appliedCount++
				}
				txsCount++
			}
			txsLock.Unlock()

			currRound := alice.ledger.Rounds().Latest().Index

			fmt.Printf("%d/%d tx applied, round=%d, prevRound=%d, root depth=%d\n",
				appliedCount, txsCount,
				currRound,
				prevRound,
				alice.ledger.Graph().RootDepth())

			if currRound-prevRound > 1 {
				t.Fatal("more than 1 round finalized")
			}

			prevRound = currRound

			if appliedCount < cap(txs) {
				assert.True(t, alice.ledger.BroadcastingNop(),
					"node should not stop broadcasting nop while there are unapplied tx")
			}

			// The test is successful if all tx are applied,
			// and nop broadcasting is stopped once all tx are applied
			if appliedCount == cap(txs) && !alice.ledger.BroadcastingNop() {
				return
			}
		}
	}
}

func TestLedger_AddTransaction(t *testing.T) {
	testnet := NewTestNetwork(t)
	defer testnet.Cleanup()

	alice := testnet.AddNode(t) // alice
	testnet.AddNode(t)          // bob

	start := alice.ledger.Rounds().Latest().Index

	assert.True(t, <-alice.WaitForSync())

	// Add just 1 transaction
	_, err := testnet.faucet.PlaceStake(100)
	assert.NoError(t, err)

	// Try to wait for 2 rounds of consensus.
	// The second call should result in timeout, because
	// only 1 round should be finalized.
	assert.True(t, <-alice.WaitForConsensus())
	assert.False(t, <-alice.WaitForConsensus())

	current := alice.ledger.Rounds().Latest().Index
	assert.Equal(t, current-start, uint64(1), "expected only 1 round to be finalized")
}

func TestLedger_Sync(t *testing.T) {
	testnet := NewTestNetwork(t)
	defer testnet.Cleanup()

	// Setup network with 3 nodes
	alice := testnet.faucet
	for i := 0; i < 2; i++ {
		testnet.AddNode(t)
	}

	// Advance the network by a few rounds larger than sys.SyncIfRoundsDifferBy
	for i := 0; i < int(sys.SyncIfRoundsDifferBy)+1; i++ {
		assert.NoError(t, txError(alice.PlaceStake(10)))
		<-alice.WaitForConsensus()
	}

	testnet.WaitForRound(t, alice.RoundIndex())

	var nodes []*TestLedger

	// When a new node joins the network, it will eventually
	// sync with the other nodes
	charlie := testnet.AddNode(t)
	nodes = append(nodes, charlie)
	time.Sleep(time.Millisecond * 100)

	eve := testnet.AddNode(t)
	nodes = append(nodes, eve)
	time.Sleep(time.Millisecond * 100)

	oscar := testnet.AddNode(t)
	nodes = append(nodes, oscar)
	time.Sleep(time.Millisecond * 100)

	timeout := time.NewTimer(time.Second * 30)
	for {
		select {
		case <-timeout.C:
			t.Fatal("timed out waiting for sync")

		default:
			var count int
			for _, node := range nodes {
				ri := <-node.WaitForRound(alice.RoundIndex())
				if ri >= alice.RoundIndex() {
					count++
				}
			}

			if count == len(nodes) {
				goto DONE
			}
		}
	}

DONE:
	assert.EqualValues(t, alice.Balance(), charlie.BalanceOfAccount(alice))
	assert.EqualValues(t, alice.Balance(), eve.BalanceOfAccount(alice))
	assert.EqualValues(t, alice.Balance(), oscar.BalanceOfAccount(alice))
}

func txError(tx Transaction, err error) error {
	return err
}
