package file

import "context"

// An IO value bundles a *File with a context so that a file can be used with
// the standard interfaces defined by the io package. An IO value should be
// used only during the lifetime of the request whose context it binds.
type IO struct {
	ctx context.Context
	f   *File
}

// Read implements the io.Reader interface.
func (io IO) Read(data []byte) (int, error) { return io.f.Read(io.ctx, data) }

// Write implements the io.Writer interface.
func (io IO) Write(data []byte) (int, error) { return io.f.Write(io.ctx, data) }

// ReadAt implements the io.ReaderAt interface.
func (io IO) ReadAt(data []byte, offset int64) (int, error) {
	return io.f.ReadAt(io.ctx, data, offset)
}

// WriteAt implments the io.WriterAt interface.
func (io IO) WriteAt(data []byte, offset int64) (int, error) {
	return io.f.WriteAt(io.ctx, data, offset)
}

// Seek implements the io.Seeker interface.
func (io IO) Seek(offset int64, whence int) (int64, error) {
	return io.f.Seek(io.ctx, offset, whence)
}

// Close implements the io.Closer interface. A File does not have a system
// descriptor, so "closing" performs a flush but does not invalidate the file.
func (io IO) Close() error { _, err := io.f.Flush(io.ctx); return err }
