package wavelet

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
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
	for <-alice.WaitForConsensus() {
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

// func TestLedger_Pay(t *testing.T) {
// 	testnet := NewTestNetwork(t)
// 	defer testnet.Cleanup()
//
// 	alice := testnet.AddNode(t, 1000000)
// 	bob := testnet.AddNode(t, 100)
//
// 	others := []*TestLedger{}
// 	for i := 0; i < 5; i++ {
// 		others = append(others, testnet.AddNode(t, 0))
// 	}
//
// 	testnet.WaitForConsensus(t)
//
// 	assert.NoError(t, txError(alice.Pay(bob, 1237)))
//
// 	testnet.WaitForConsensus(t)
//
// 	// Bob should receive the tx amount
// 	assert.EqualValues(t, 1337, bob.Balance())
//
// 	// Alice balance should be balance-txAmount-gas
// 	aliceBalance := alice.Balance()
// 	assert.True(t, aliceBalance < 1000000-1237)
//
// 	// Everyone else should see the updated balance of Alice and Bob
// 	for _, node := range others {
// 		assert.EqualValues(t, aliceBalance, node.BalanceOfAccount(alice))
// 		assert.EqualValues(t, 1337, node.BalanceOfAccount(bob))
// 	}
// }
//
// func TestLedger_PayInsufficientBalance(t *testing.T) {
// 	testnet := NewTestNetwork(t)
// 	defer testnet.Cleanup()
//
// 	alice := testnet.AddNode(t, 1000000)
// 	bob := testnet.AddNode(t, 100)
//
// 	others := []*TestLedger{}
// 	for i := 0; i < 5; i++ {
// 		others = append(others, testnet.AddNode(t, 0))
// 	}
//
// 	testnet.WaitForConsensus(t)
//
// 	// Alice attempt to pay Bob more than what
// 	// she has in her wallet
// 	assert.NoError(t, txError(alice.Pay(bob, 1000001)))
//
// 	testnet.WaitForConsensus(t)
//
// 	// Bob should not receive the tx amount
// 	assert.EqualValues(t, 100, bob.Balance())
//
// 	// Alice should have paid for gas even though the tx failed
// 	aliceBalance := alice.Balance()
// 	assert.True(t, aliceBalance > 0)
// 	assert.True(t, aliceBalance < 1000000)
//
// 	// Everyone else should see the updated balance of Alice and Bob
// 	for _, node := range others {
// 		assert.EqualValues(t, aliceBalance, node.BalanceOfAccount(alice))
// 		assert.EqualValues(t, 100, node.BalanceOfAccount(bob))
// 	}
// }
//
// func TestLedger_Gossip(t *testing.T) {
// 	testnet := NewTestNetwork(t)
// 	defer testnet.Cleanup()
//
// 	alice := testnet.AddNode(t, 1000000)
// 	bob := testnet.AddNode(t, 100)
//
// 	for i := 0; i < 5; i++ {
// 		testnet.AddNode(t, 0)
// 	}
//
// 	testnet.WaitForConsensus(t)
//
// 	assert.NoError(t, txError(alice.Pay(bob, 1237)))
//
// 	testnet.WaitForConsensus(t)
//
// 	// When a new node joins the network, it will eventually receive
// 	// all transactions in the network.
// 	charlie := testnet.AddNode(t, 0)
// 	timeout := time.NewTimer(15 * time.Second)
// 	ticker := time.NewTicker(200 * time.Millisecond)
//
// 	for {
// 		select {
// 		case <-timeout.C:
// 			t.Fatal("test timed out")
//
// 		case <-ticker.C:
// 			if charlie.BalanceOfAccount(alice) == alice.Balance() &&
// 				charlie.BalanceOfAccount(bob) == bob.Balance() {
// 				return
// 			}
// 		}
// 	}
// }
//
// func TestLedger_Stake(t *testing.T) {
// 	testnet := NewTestNetwork(t)
// 	defer testnet.Cleanup()
//
// 	alice := testnet.AddNode(t, 1000000)
//
// 	others := []*TestLedger{}
// 	for i := 0; i < 5; i++ {
// 		others = append(others, testnet.AddNode(t, 0))
// 	}
//
// 	testnet.WaitForConsensus(t)
//
// 	assert.NoError(t, txError(alice.PlaceStake(9001)))
// 	testnet.WaitForConsensus(t)
//
// 	assert.EqualValues(t, 9001, alice.Stake())
//
// 	// Alice balance should be balance-stakeAmount-gas
// 	aliceBalance := alice.Balance()
// 	assert.True(t, aliceBalance < 1000000-9001)
//
// 	// Everyone else should see the updated balance of Alice
// 	for _, node := range others {
// 		assert.EqualValues(t, aliceBalance, node.BalanceOfAccount(alice))
// 		assert.EqualValues(t, alice.Stake(), node.StakeOfAccount(alice))
// 	}
//
// 	assert.NoError(t, txError(alice.WithdrawStake(5000)))
// 	testnet.WaitForConsensus(t)
//
// 	assert.EqualValues(t, 4001, alice.Stake())
//
// 	// Withdrawn stake should be added to balance
// 	oldBalance := aliceBalance
// 	aliceBalance = alice.Balance()
// 	assert.True(t, aliceBalance > oldBalance)
//
// 	// Everyone else should see the updated balance of Alice
// 	for _, node := range others {
// 		assert.EqualValues(t, aliceBalance, node.BalanceOfAccount(alice))
// 		assert.EqualValues(t, alice.Stake(), node.StakeOfAccount(alice))
// 	}
// }
//
// func TestLedger_Reward(t *testing.T) {
// 	testnet := NewTestNetwork(t)
// 	defer testnet.Cleanup()
//
// 	alice := testnet.AddNode(t, 1000000)
// 	bob := testnet.AddNode(t, 1000000)
// 	charlie := testnet.AddNode(t, 1000000)
//
// 	others := []*TestLedger{}
// 	for i := 0; i < 10; i++ {
// 		others = append(others, testnet.AddNode(t, 0))
// 	}
//
// 	testnet.WaitForConsensus(t)
//
// 	assert.EqualValues(t, 0, alice.Reward())
// 	assert.EqualValues(t, 0, bob.Reward())
//
// 	assert.NoError(t, txError(alice.PlaceStake(500000)))
// 	testnet.WaitForConsensus(t)
// 	assert.NoError(t, txError(bob.PlaceStake(500000)))
// 	testnet.WaitForConsensus(t)
// 	assert.NoError(t, txError(charlie.PlaceStake(sys.MinimumStake-1)))
// 	testnet.WaitForConsensus(t)
//
// 	// Run through several rounds of consensus
// 	from := []*TestLedger{alice, bob}
// 	for i := 0; i < 10; i++ {
// 		// Generate some transactions
// 		assert.NoError(t, txError(from[i%2].Pay(others[i%10], uint64(rand.Intn(200)))))
// 		testnet.WaitForConsensus(t)
// 	}
//
// 	// Alice and Bob should have received some rewards
// 	assert.True(t, alice.Reward() > 0)
// 	assert.True(t, bob.Reward() > 0)
//
// 	// Charlie shouldn't receive rewards because his stake is too little
// 	assert.EqualValues(t, 0, charlie.Reward())
// }

func txError(tx Transaction, err error) error {
	return err
}
