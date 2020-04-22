package paych

import (
	"math"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
)

// Maximum number of lanes in a channel.
const MaxLane = math.MaxInt64

const SettleDelay = builtin.EpochsInHour * 12

// Maximum size of a secret that can be submitted with a payment channel update (in bytes).
const MaxSecretSize = 256
