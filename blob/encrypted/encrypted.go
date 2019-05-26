// Package encrypted implements an encrypted blob store in which blobs are
// encrypted with a block cipher in CTR mode. Blob storage is delegated to an
// underlying blob.Store implementation, to which the encryption is opaque.
package encrypted

import (
	"context"
	"crypto/cipher"
	"crypto/rand"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/encrypted/encpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"golang.org/x/xerrors"
)

// A Store implements the blob.Store interface and encrypts blob data using a
// block cipher in CTR mode. Blob storage is delegated to an underlying store.
//
// Note that keys are not encrypted, only block contents.
type Store struct {
	blk   cipher.Block       // used to generate the keystream
	newIV func([]byte) error // generate a fresh initialization vector
	real  blob.Store         // the underlying storage implementation
}

// Options control the construction of a *Store.
type Options struct {
	// Replace the contents of iv with fresh initialization vector.
	// If nil, the store uses the crypto/rand package to generate random IVs.
	NewIV func(iv []byte) error
}

func (o *Options) newIV() func([]byte) error {
	if o != nil && o.NewIV != nil {
		return o.NewIV
	}
	return func(iv []byte) error {
		_, err := rand.Read(iv)
		return err
	}
}

// New constructs a new encrypted store that delegates to s.  If opts == nil,
// default options are used.  New will panic if s == nil or blk == nil.
func New(s blob.Store, blk cipher.Block, opts *Options) *Store {
	return &Store{
		blk:   blk,
		newIV: opts.newIV(),
		real:  s,
	}
}

// Get implements part of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	enc, err := s.load(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.decrypt(enc)
}

// Put implements part of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	enc, err := s.encrypt(opts.Data)
	if err != nil {
		return err
	}
	bits, err := proto.Marshal(enc)
	if err != nil {
		return err
	}

	// Leave the original options as given, but replace the data.
	opts.Data = bits
	return s.real.Put(ctx, opts)
}

// Size implements part of the blob.Store interface. This implementation
// requires access to the blob content, since the stored size of an encrypted
// blob is not equivalent to the original.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	enc, err := s.load(ctx, key)
	if err != nil {
		return 0, err
	}
	return enc.UncompressedSize, nil
}

// Delete implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Delete(ctx context.Context, key string) error { return s.real.Delete(ctx, key) }

// List implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, f)
}

// Len implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }

// load fetches a stored block and decodes its storage wrapper.
func (s *Store) load(ctx context.Context, key string) (*encpb.Encrypted, error) {
	bits, err := s.real.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	pb := new(encpb.Encrypted)
	if err := proto.Unmarshal(bits, pb); err != nil {
		return nil, err
	}
	return pb, nil
}

// encrypt compresses and encrypts the given data and returns its storage wrapper.
func (s *Store) encrypt(data []byte) (*encpb.Encrypted, error) {
	compressed := snappy.Encode(data, nil)
	iv := make([]byte, s.blk.BlockSize())
	if err := s.newIV(iv); err != nil {
		return nil, xerrors.Errorf("encrypt: initialization vector: %w", err)
	}
	ctr := cipher.NewCTR(s.blk, iv)
	ctr.XORKeyStream(compressed, compressed)
	return &encpb.Encrypted{
		Data:             compressed,
		Init:             iv,
		UncompressedSize: int64(len(data)),
	}, nil
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (s *Store) decrypt(enc *encpb.Encrypted) ([]byte, error) {
	ctr := cipher.NewCTR(s.blk, enc.Init)
	ctr.XORKeyStream(enc.Data, enc.Data)
	decompressed, err := snappy.Decode(enc.Data, nil)
	if err != nil {
		return nil, xerrors.Errorf("decrypt: decompress: %w", err)
	} else if int64(len(decompressed)) != enc.UncompressedSize {
		return nil, xerrors.Errorf("decrypt: wrong size (got %d, want %d)",
			len(decompressed), enc.UncompressedSize)
	}
	return decompressed, nil
}

/*
Implementation notes

An encrypted block is stored as an encpb.Encrypted protocol buffer, encrypted
with AES in CTR mode. The block data are compressed with snappy [1] prior to
encryption.

[1]: https://github.com/google/snappy
*/
