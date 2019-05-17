package splitter

// A RollingHash implements a rolling hash function over a window of byte data.
type RollingHash interface {
	// Reset restores the hash to its initial state.
	Reset()

	// Update shifts b into the window and returns the updated hash value.
	Update(b byte) uint

	// Size returns the size of the rolling hash window in bytes.
	Size() int
}

// DefaultHash is a Rabin-Karp rolling hash with default settings.
func DefaultHash() RollingHash { return RabinKarpHash(1031, 2147483659, 48) }

type modHash struct {
	hash uint   // Current hash state.
	base int    // Base for exponentiation.
	mod  int    // Modulus, should usually be prime.
	inv  int    // Base shifted by size-1 bytes, for subtraction.
	buf  []byte // Window buffer.
	next int    // Offset in window of next free position.
}

// RabinKarpHash returns a Hasher implementation that uses the Rabin-Karp
// rolling hash construction with the given base and modulus.
func RabinKarpHash(base, modulus, windowSize int) RollingHash {
	return &modHash{
		base: base,
		mod:  modulus,
		inv:  exptmod(base, windowSize-1, modulus),
		buf:  make([]byte, windowSize),
	}
}

// Reset resets m to its initial configuration.
func (m *modHash) Reset() {
	m.hash = 0
	m.next = 0
}

// Update implements a required method of the RollingHash interface.
func (m *modHash) Update(b byte) uint {
	out := m.buf[m.next]
	m.buf[m.next] = b
	m.next = (m.next + 1) % len(m.buf)

	h := m.base*(int(m.hash)-m.inv*int(out)) + int(b)
	h %= m.mod
	if h < 0 {
		h += m.mod
	}
	m.hash = uint(h)
	return m.hash
}

// Size implements a required method of the RollingHash interface.
func (m *modHash) Size() int { return len(m.buf) }

// exptmod(b, e, m) computes b**e modulo m.
func exptmod(b, e, m int) int {
	s := 1
	for e != 0 {
		if e&1 == 1 {
			s = (s * b) % m
		}
		b = (b * b) % m
		e >>= 1
	}
	return s
}
