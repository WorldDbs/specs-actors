package testing

import "github.com/filecoin-project/go-state-types/abi"

func MakePID(input string) abi.PeerID {
	return abi.PeerID([]byte(input))
}
