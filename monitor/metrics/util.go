package metrics

import (
	"github.com/ipfs/ipfs-cluster/api"

	peer "github.com/libp2p/go-libp2p-peer"
)

// PeersetFilter removes all metrics not belonging to the given
// peerset
func PeersetFilter(metrics []api.Metric, peerset []peer.ID) []api.Metric {
	peerMap := make(map[string]struct{})
	for _, pid := range peerset {
		peerMap[peer.IDB58Encode(pid)] = struct{}{}
	}

	filtered := make([]api.Metric, 0, len(metrics))

	for _, metric := range metrics {
		_, ok := peerMap[metric.Peer]
		if !ok {
			continue
		}
		filtered = append(filtered, metric)
	}

	return filtered
}
