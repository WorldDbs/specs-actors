package agent

import (
	"bytes"
	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// extracted from lotus/chain/vm/vm.go

func BlockstoreCopy(from, to cbor.IpldBlockstore, root cid.Cid) (blocks uint64, copySize uint64, err error) {
	var numBlocks uint64
	var totalCopySize uint64

	cp := func(blk block.Block) error {
		numBlocks++
		totalCopySize += uint64(len(blk.RawData()))
		return to.Put(blk)
	}

	if err := copyRec(from, to, root, cp); err != nil {
		return 0, 0, xerrors.Errorf("copyRec: %w", err)
	}

	return numBlocks, totalCopySize, nil
}

func copyRec(from, to cbor.IpldBlockstore, root cid.Cid, cp func(block.Block) error) error {
	if root.Prefix().MhType == 0 {
		// identity cid, skip
		return nil
	}

	blk, err := from.Get(root)
	if err != nil {
		return xerrors.Errorf("get %s failed: %w", root, err)
	}

	var lerr error
	err = linksForObj(blk, func(link cid.Cid) {
		if lerr != nil {
			// Theres no error return on linksForObj callback :(
			return
		}

		prefix := link.Prefix()
		if prefix.Codec == cid.FilCommitmentSealed || prefix.Codec == cid.FilCommitmentUnsealed {
			return
		}

		// We always have blocks inlined into CIDs, but we may not have their children.
		if prefix.MhType == mh.IDENTITY {
			// Unless the inlined block has no children.
			if prefix.Codec == cid.Raw {
				return
			}
		}

		if err := copyRec(from, to, link, cp); err != nil {
			lerr = err
			return
		}
	})
	if err != nil {
		return xerrors.Errorf("linksForObj (%x): %w", blk.RawData(), err)
	}
	if lerr != nil {
		return lerr
	}

	if err := cp(blk); err != nil {
		return xerrors.Errorf("copy: %w", err)
	}
	return nil
}

func linksForObj(blk block.Block, cb func(cid.Cid)) error {
	switch blk.Cid().Prefix().Codec {
	case cid.DagCBOR:
		err := cbg.ScanForLinks(bytes.NewReader(blk.RawData()), cb)
		if err != nil {
			return xerrors.Errorf("cbg.ScanForLinks: %w", err)
		}
		return nil
	case cid.Raw:
		// We implicitly have all children of raw blocks.
		return nil
	default:
		return xerrors.Errorf("vm flush copy method only supports dag cbor")
	}
}
