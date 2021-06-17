package rocks

// Operator easy to operate rocksdb
type Operator struct {
	db   *DB
	opts *Options

	ropt *ReadOptions
	wopt *WriteOptions

	cfhs map[string]*ColumnFamilyHandle

	isDestory bool
}

// GetSelf get object  *DB *ColumnFamilyHandle
func (op *Operator) GetSelf() (*DB, map[string]*ColumnFamilyHandle) {
	return op.db, op.cfhs
}

// SetDefaultWriteOptions set deufalt WriteOptions
func (op *Operator) SetDefaultWriteOptions(wopt *WriteOptions) {
	if op.wopt != nil {
		op.wopt.Destroy()
	}
	op.wopt = wopt
}

// SetDefaultReadOptions set deufalt ReadOptions
func (op *Operator) SetDefaultReadOptions(ropt *ReadOptions) {
	if op.ropt != nil {
		op.ropt.Destroy()
	}
	op.ropt = ropt
}

// ColumnFamily return a OperatorColumnFamily
func (op *Operator) ColumnFamily(name string) *OperatorColumnFamily {
	if cfh, ok := op.cfhs[name]; ok {
		if op.wopt == nil {
			op.wopt = NewDefaultWriteOptions()
		}

		if op.ropt == nil {
			op.ropt = NewDefaultReadOptions()
		}

		return &OperatorColumnFamily{
			db:   op.db,
			wopt: op.wopt,
			ropt: op.ropt,
			cfh:  cfh,
		}
	}
	return nil
}

// CreateColumnFamily create a ColumnFamily
func (op *Operator) CreateColumnFamily(name string) error {
	cf, err := op.db.CreateColumnFamily(op.opts, name)
	if err != nil {
		return nil
	}
	op.cfhs[name] = cf
	return nil
}

// Operator easy to operate rocksdb
type OperatorColumnFamily struct {
	db   *DB
	ropt *ReadOptions
	wopt *WriteOptions
	cfh  *ColumnFamilyHandle
}

// Get Slice is need free
func (opcf *OperatorColumnFamily) Get(key []byte) (*Slice, error) {
	return opcf.db.GetCF(opcf.ropt, opcf.cfh, key)
}

// Get Slices is need Destory
func (opcf *OperatorColumnFamily) MultiGetCF(key []byte) (Slices, error) {
	return opcf.db.MultiGetCF(opcf.ropt, opcf.cfh, key)
}

func (opcf *OperatorColumnFamily) GetSafe(key []byte, do func(value []byte)) error {
	s, err := opcf.db.GetCF(opcf.ropt, opcf.cfh, key)
	if err != nil {
		return err
	}
	defer s.Free()
	do(s.Data())
	return nil
}

func OpenDbColumnFamiliesEx(opts *Options, name string) (*Operator, error) {

	cfnames, err := ListColumnFamilies(opts, name)
	if err != nil {
		return nil, err
	}

	var cfOpts []*Options
	for range cfnames {
		cfOpts = append(cfOpts, opts)
	}

	db, cfs, err := OpenDbColumnFamilies(opts, name, cfnames, cfOpts)
	if err != nil {
		return nil, err
	}

	op := &Operator{
		db:   db,
		opts: opts,
		cfhs: make(map[string]*ColumnFamilyHandle),
	}
	for i, name := range cfnames {
		op.cfhs[name] = cfs[i]
	}

	return op, nil
}
