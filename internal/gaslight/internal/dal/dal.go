package dal

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	metadataPageID = 0
)

type Serializer interface {
	Serialize([]byte)
}

type Deserializer interface {
	Deserialize([]byte)
}

func New(ds Datasource) (*DAL, error) {
	dal := &DAL{
		ds: ds,
		freelist: &freelist{
			allocated: 1, // one page is pre-allocated for metadata
		},
		metadata: &metadata{},
		pageSize: uint64(os.Getpagesize()),
	}
	id := dal.freelist.id()
	if err := dal.Serialize(dal.freelist, id); err != nil {
		return nil, err
	}
	dal.metadata.freelist = id
	if err := dal.Serialize(dal.metadata, metadataPageID); err != nil {
		return nil, err
	}
	return dal, nil
}

func Load(ds Datasource) (*DAL, error) {
	dal := &DAL{
		ds:       ds,
		freelist: &freelist{},
		metadata: &metadata{},
		pageSize: uint64(os.Getpagesize()),
	}
	err := dal.Deserialize(dal.metadata, metadataPageID)
	if err != nil {
		return nil, err
	}
	err = dal.Deserialize(dal.freelist, dal.metadata.freelist)
	if err != nil {
		return nil, err
	}
	return dal, nil
}

type Datasource interface {
	io.ReadWriteCloser
	io.Seeker
}

type DAL struct {
	ds Datasource
	*freelist
	*metadata
	pageSize uint64
}

type freelist struct {
	allocated uint64
	released  []uint64
}

func (f *freelist) id() uint64 {
	if len(f.released) == 0 {
		f.allocated += 1
		return f.allocated
	}
	next := f.released[len(f.released)-1]
	released := make([]uint64, 0, len(f.released)-1)
	copy(released, f.released[:len(f.released)-1])
	f.released = released
	return next
}

func (f *freelist) release(id uint64) {
	f.released = append(f.released, id)
}

func (f *freelist) Serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], f.allocated)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:], uint64(len(f.released)))
	pos += 8
	for _, id := range f.released {
		binary.LittleEndian.PutUint64(buf[pos:], id)
		pos += 8
	}
}

func (f *freelist) Deserialize(buf []byte) {
	pos := 0
	f.allocated = binary.LittleEndian.Uint64(buf[pos:])
	pos += 8
	f.released = make([]uint64, 0, binary.LittleEndian.Uint64(buf[pos:]))
	pos += 8
	for i := 0; i < cap(f.released); i++ {
		f.released = append(f.released, binary.LittleEndian.Uint64(buf[pos:]))
		pos += 8
	}
}

type metadata struct {
	freelist uint64
}

func (m *metadata) Serialize(buf []byte) {
	binary.LittleEndian.PutUint64(buf, m.freelist)
}

func (m *metadata) Deserialize(buf []byte) {
	m.freelist = binary.LittleEndian.Uint64(buf)
}

type page struct {
	id   uint64
	data []byte
}

func (d *DAL) allocate() *page {
	return &page{
		data: make([]byte, d.pageSize),
	}
}

func (d *DAL) read(id uint64) (*page, error) {
	p := d.allocate()
	offset := id * d.pageSize
	_, err := d.ds.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	_, err = d.ds.Read(p.data)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *DAL) write(p *page) error {
	offset := p.id * d.pageSize
	_, err := d.ds.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.ds.Write(p.data)
	return err
}

func (d *DAL) Serialize(serializable Serializer, id uint64) error {
	p := d.allocate()
	p.id = id
	serializable.Serialize(p.data)
	err := d.write(p)
	if err != nil {
		return err
	}
	return nil
}

func (d *DAL) Deserialize(deserializer Deserializer, id uint64) error {
	p, err := d.read(id)
	if err != nil {
		return err
	}
	deserializer.Deserialize(p.data)
	return nil
}

func (d *DAL) Close() error {
	if d.ds == nil {
		return nil
	}
	if err := d.Serialize(d.freelist, d.metadata.freelist); err != nil {
		return err
	}
	if err := d.Serialize(d.metadata, metadataPageID); err != nil {
		return err
	}
	return nil
}

// serializer is a small utility that aids in serializing complex values to byte slices, it keeps track of the current
// position being written to and which direction the cursor should move after each write (forwards or backwards).
type serializer struct {
	position  int
	direction int
	buffer    []byte
}

func (s *serializer) PutUint64(x uint64) {
	if s.direction < 0 {
		s.position += 8 * s.direction
	}
	binary.LittleEndian.PutUint64(s.buffer[s.position:], x)
	if s.direction > 0 {
		s.position += 8 * s.direction
	}
}

func (s *serializer) PutUint16(x uint16) {
	if s.direction < 0 {
		s.position += 2 * s.direction
	}
	binary.LittleEndian.PutUint16(s.buffer[s.position:], x)
	if s.direction > 0 {
		s.position += 2 * s.direction
	}
}

func (s *serializer) PutUint8(x uint8) {
	if s.direction < 0 {
		s.position += 1 * s.direction
	}
	s.buffer[s.position] = x
	if s.direction > 0 {
		s.position += 1 * s.direction
	}
}

func (s *serializer) Put(bytes []byte) {
	if s.direction < 0 {
		s.position += len(bytes) * s.direction
	}
	copy(s.buffer[s.position:], bytes)
	if s.direction > 0 {
		s.position += len(bytes) * s.direction
	}
}

func (s *serializer) Step(steps int) {
	s.position += steps * s.direction
}

func (s *serializer) Position() int {
	return s.position
}
