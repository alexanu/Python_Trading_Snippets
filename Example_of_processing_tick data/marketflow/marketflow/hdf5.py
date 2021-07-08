'''Work with TAQ data and HDF5 files using pytables

We're not sure this is the best way to go, but it's a reasonable place to start
for a standard binary format'''

from os import path

import numpy as np
import tables as tb

from .processing import SplitChunks
from . import TAQ2Chunks


class H5Writer:
    '''Set up an hdf5 file, write tables from numpy struct arrays.

    If a table already exists, default behavior is to append.
    '''

    # Note that the table description will be constructed based on the dtype of
    # the first chunk seen by `.append()`.
    tb_desc = None

    def __init__(self, h5_fname, title=None, filters=None):
        '''Get ready to write to HDF5

        Note that the table description will be constructed based on the dtype
        of the first chunk seen by `.append()`.

        h5_fname : str
            Used to make default `title` of HDF5 file, and the filename.
        title : str
            Specify the `title` of HDF5 file.
        filters : tb.Filters
            How to compress, etc.?
        '''
        if title is None:
            bname = path.basename(h5_fname)
            title, _ = path.splitext(bname)

        if filters is None:
            filters = tb.Filters(complevel=9,
                                 complib='blosc:lz4hc',
                                 fletcher32=True)

        # We're using aggressive compression and checksums, since this will
        # likely stick around. That said, checksums are likely redundant with
        # ZFS, and perhaps LZ4 is too. Moreover, blosc makes it harder to use
        # this file with any runtime.
        self.h5 = tb.open_file(h5_fname, title=title, mode='w',
                               filters=filters)

    def finalize_hdf5(self):
        self.h5.close()

    def set_table_type(self, target_dtype):
        '''Convert NumPy dtype to PyTable descriptor (adapted from
        blaze.pytables).  E.g.:
        --------
        >>> dt = np.dtype([('name', 'S7'), ('amount', 'i4'), ('time', 'M8[us]')])
        >>> this.set_table_type(dt)  # doctest: +SKIP
        {'amount': Int32Col(shape=(), dflt=0, pos=1),
         'name': StringCol(itemsize=7, shape=(), dflt='', pos=0)}
        '''
        # This lets us deal with things like python list based specifications
        target_dtype = np.dtype(target_dtype)

        tb_desc = {}
        for pos, name in enumerate(target_dtype.names):
            dt, _ = target_dtype.fields[name]
            # We may eventually want to deal with datetime64 columns, but for
            # now, we don't need to
            # if issubclass(dt.type, np.datetime64):
            #     desc = tb.Description({name: tb.Time64Col(pos=pos)})
            # else:
            desc, _ = tb.descr_from_dtype(np.dtype([(name, dt)]))
            getattr(desc, name)._v_pos = pos
            tb_desc.update(desc._v_colobjects)

        self.tb_desc = tb_desc

    def append(self, path, name, data):
        '''Put `data` in a table at `path`. Create the table if needed.

        path : str
            '/'-separated path from the root
        data : compatible array/buffer object
            E.g., numpy structured array

        XXX currently, we are not being very smart about chunkshape. We should
        revisit. If we get two chunks for the same location, but with different
        dtypes, this function will try to do an append that won't work!
        '''
        # This is an optimization to avoid computing the tb_desc too many times
        if self.tb_desc is None or self.source_dtype != data.dtype:
            self.source_dtype = data.dtype
            self.set_table_type(self.source_dtype)

        try:
            table = self.h5.get_node(path, name)
            # This will generate an error if the previous chunk had a different
            # dtype
            table.append(data)
        except tb.NoSuchNodeError:
            self.h5.create_table(path, name, description=self.tb_desc,
                                 createparents=True, obj=data)


def conv_to_hdf5(taq_name, h5_name):
    '''Read raw bytes from TAQ, write to HDF5'''
    taq_in = TAQ2Chunks(taq_name, do_process_chunk=True)

    # XXX Should I use a context manager here?
    h5writer = H5Writer(h5_name)
    # XXX We set sorted_cols to False here for now so things work on our test
    # data.  Should revert this.
    split = SplitChunks(taq_in, ['Symbol_root', 'Symbol_suffix'],
                        drop_columns=True, sorted_cols=False)

    try:
        for security, chunk in split:
            symbol, suffix = security
            path = '/' + symbol.strip().decode('ascii')

            # Clean up our suffix, including special cases
            if suffix.isspace():
                suffix = 'no_suffix'
            else:
                suffix = suffix.strip().decode('ascii')
                # This happens in at least EQY_US_ALL_BBO_20140206.zip
                # It appears to be undocumented
                if suffix == '.':
                    suffix = 'dot'

            h5writer.append(path, suffix, chunk)

    # We want to make sure we close our file nicely (though I'm pretty sure
    # pytables handles this anyway...)
    finally:
        h5writer.finalize_hdf5()

    # I care less about closing the taq_in file...


def taq2h5(overwrite=False):
    '''Basic conversion from zip file to HDF5, use like this:

    $ taq2h5 ../../local_data/EQY_US_ALL_BBO_201502*.zip

    (It's installed as a package script)
    '''
    from os import path
    from argparse import ArgumentParser

    from .utility import timeit

    parser = ArgumentParser()
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing .h5 files')
    parser.add_argument('fnames', nargs='+', metavar='filename',
                        help='A TAQ file to convert')
    parsed = parser.parse_args()

    timed_conv = timeit(conv_to_hdf5)

    for name in parsed.fnames:
        h5_name, _ = path.splitext(name)
        h5_name += '.h5'
        print('converting {} to {}'.format(name, h5_name))

        if (not parsed.overwrite) and path.exists(h5_name):
            print('skipping, {} exists'.format(h5_name))
        else:
            timed_conv(name, h5_name)
