'''Basic, efficient interface to TAQ data using numpy

A central design goal is minimizing external dependencies
'''

from zipfile import ZipFile

from pytz import timezone
import numpy as np
from datetime import datetime


class BytesSpec(object):
    '''A description of the records in raw TAQ files'''

    # List of (Name, # of bytes_spectes)
    # We will use this to contstuct "bytes" (which is what 'S' stands for - it
    # doesn't stand for "string")
    initial_dtype_info = [
                          # Time is given in HHMMSSmmm, should be in Eastern
                          # Time (ET)
                          ('hour', 2),
                          ('minute', 2),
                          ('msec', 5),  # This includes seconds - so up to
                                        # 59,999 msecs
                          ('Exchange', 1),
                          ('Symbol_root', 6),
                          ('Symbol_suffix', 10),
                          ('Bid_Price', 11),  # 7.4 (fixed point)
                          ('Bid_Size', 7),
                          ('Ask_Price', 11),  # 7.4
                          ('Ask_Size', 7),
                          ('Quote_Condition', 1),
                          # Market_Maker ends up getting discarded
                          # It should always be b'    '
                          ('Market_Maker', 4),
                          ('Bid_Exchange', 1),
                          ('Ask_Exchange', 1),
                          ('Sequence_Number', 16),
                          ('National_BBO_Ind', 1),
                          ('NASDAQ_BBO_Ind', 1),
                          ('Quote_Cancel_Correction', 1),
                          ('Source_of_Quote', 1),
                          ('Retail_Interest_Indicator_RPI', 1),
                          ('Short_Sale_Restriction_Indicator', 1),
                          ('LULD_BBO_Indicator_CQS', 1),
                          ('LULD_BBO_Indicator_UTP', 1),
                          ('FINRA_ADF_MPID_Indicator', 1),
                          ('SIP_generated_Message_Identifier', 1),
                          ('National_BBO_LULD_Indicator', 1),
                         ]

    # Justin and Pandas (I think) use time64, as does PyTables.
    # We could use msec from beginning of day for now in an uint16
    # (maybe compare performance to datetime64? Dates should compress very
    # well...)

    # The following should all *always* be in all TAQ formats
    convert_dtype = [
                     # Time is the first field in HHMMSSmmm format
                     ('hour', np.int8),
                     ('minute', np.int8),
                     # This works well for now, but pytables wants:
                     # <seconds-from-epoch>.<fractional-seconds> as a float64
                     ('msec', np.uint16),
                     ('Bid_Price', np.float64),
                     ('Bid_Size', np.int32),
                     ('Ask_Price', np.float64),
                     ('Ask_Size', np.int32),
                     # This is not currently used, and should always be b'    '
                     # ('Market_Maker', np.int8),
                     ('Sequence_Number', np.int64),
                     # The _Ind fields are actually categorical,
                     # leaving as strings for now. If converting to pandas, use
                     # categorical type!
                     # ('National_BBO_Ind', np.int8),
                     # ('NASDAQ_BBO_Ind', np.int8),
                    ]

    convert_dict = dict(convert_dtype)

    # This gets reduced to only present fields in .check_present_fields()
    # These are enumerated explicitly to allow for fields like Time, that may
    # be dynamically computed
    passthrough_strings = ['Exchange',
                           'Symbol_root',
                           'Symbol_suffix',
                           'Quote_Condition',
                           'Market_Maker',
                           'Bid_Exchange',
                           'Ask_Exchange',
                           'National_BBO_Ind',
                           'NASDAQ_BBO_Ind',
                           'Quote_Cancel_Correction',
                           'Source_of_Quote',
                           'Retail_Interest_Indicator_RPI',
                           'Short_Sale_Restriction_Indicator',
                           'LULD_BBO_Indicator_CQS',
                           'LULD_BBO_Indicator_UTP',
                           'FINRA_ADF_MPID_Indicator',
                           'SIP_generated_Message_Identifier',
                           'National_BBO_LULD_Indicator'
                           ]

    @property
    def target_dtype(self):
        '''We're being careful about operations on this value!'''
        return self._target_dtype.copy()

    def __init__(self, bytes_per_line, computed_fields=None):
        '''Set up dtypes, etc. based on bytes_per_line

        bytes_per_line : int
            Should be one of two possible values XXX which are?
        computed_fields : [('Name', dtype), ...]
            A list-based structured dtype, for use for example with
            `[('Time', 'datetime64[ms]')]`.  PyTables will not accept
            np.datetime64, we need to pass a float64 in, and let pytables
            convert to time64.
        '''
        self.bytes_per_line = bytes_per_line
        self.check_present_fields()

        # The "easy" dtypes are the "not datetime" dtypes
        easy_dtype = []

        for name, dtype in self.initial_dtype:
            if name in self.convert_dict:
                easy_dtype.append( (name, self.convert_dict[name]) )
            elif name in self.passthrough_strings:
                easy_dtype.append( (name, dtype) )
            # Items not in these strings are silently ignored! We could add
            # logic to allow for explicitly ignoring fields here.

        if computed_fields:
            self._target_dtype = computed_fields + easy_dtype
        else:
            self._target_dtype = easy_dtype

    def check_present_fields(self):
        """
        self.initial_dtype_info should be of form, we encode newline info here!

        [('Time', 9),
         ('Exchange', 1),
         ...
        ]

        Assumption is that the last field is a newline field that is present in
        all versions of BBO
        """
        cum_len = 0
        self.initial_dtype = []
        present_passthrough = []

        # Newlines consume 2 bytes
        target_len = self.bytes_per_line - 2

        for field_name, field_len in self.initial_dtype_info:
            # Better to do nested unpacking within the function
            cum_len += field_len
            self.initial_dtype.append( (field_name, 'S{}'.format(field_len)) )
            if field_name in self.passthrough_strings:
                present_passthrough.append(field_name)
            if cum_len == target_len:
                self.initial_dtype.append(('newline', 'S2'))
                self.passthrough_strings = present_passthrough
                return

        raise BaseException("Can't map fields onto bytes_per_line")


class TAQ2Chunks:
    '''Read in raw TAQ BBO file, and return numpy chunks (cf. odo)'''

    # These are the data available in the header of the file
    numlines = None
    year = None
    month = None
    day = None
    # This isn't available in our chunks, so we'll expose it here
    first_line = None

    # This is a totally random guess. It should probably be tuned if we care...
    DEFAULT_CHUNKSIZE = 1000000

    def __init__(self, taq_fname, chunksize=None, do_process_chunk=True):
        '''Configure conversion process and (for now) set up the iterator
        taq_fname : str
            Name of input file
        chunksize : int
            Number of rows in each chunk. If None, the HDF5 logic will set it
            based on the chunkshape determined by pytables. Otherwise,
            `chunks()` will set this to DEFAULT_CHUNKSIZE.
        do_process_chunk : bool
            Do type conversions?
        chunk_type : read in by chunksize "lines" or by unbroken run of
            stock "symbols"
        '''
        self.taq_fname = taq_fname
        self.chunksize = chunksize
        self.do_process_chunk = do_process_chunk

        self._iterator = self._convert_taq()
        # Get first line read / set up remaining attributes
        next(self._iterator)

    def __len__(self):
        return self.numlines

    def __iter__(self):
        # Returning the internal iterator avoids a function call, not a big
        # deal, but may as well avoid extra computation
        return self._iterator

    def __next__(self):
        return next(self._iterator)

    def _convert_taq(self):
        '''Return a generator that yields chunks, based on config in object

        This is meant to be called from within `__init__()`, and stored in
        `self._iterator`
        '''
        # The below doesn't work for pandas (and neither does `unzip` from the
        # command line). Probably want to use something like `7z x -so
        # my_file.zip 2> /dev/null` if we use pandas.

        with ZipFile(self.taq_fname) as zfile:
            # We unpack a single-item sequence with the comma
            # XXX Should maybe add better exception handling here
            self.infile_name, = zfile.namelist()

            with zfile.open(self.infile_name) as infile:
                # this is part of the public interface
                self.first_line = infile.readline()
                bytes_per_line = len(self.first_line)

                if self.do_process_chunk:
                    self.bytes_spec = \
                        BytesSpec(bytes_per_line,
                                  computed_fields=[('Time', np.float64)])
                else:
                    self.bytes_spec = BytesSpec(bytes_per_line)

                # You need to use bytes to split bytes
                # some files (probably older files do not have a record count)
                try:
                    dateish, numlines = self.first_line.split(b":")
                    self.numlines = int(numlines)
                except ValueError:
                    dateish = self.first_line

                # Get dates to combine with times later
                # This is a little over-trusting of the spec...
                self.month = int(dateish[2:4])
                self.day = int(dateish[4:6])
                self.year = int(dateish[6:10])

                # Nice idea from @rdhyee, we only need to compute the
                # 0-second for the day once per file.self
                naive_dt = datetime(self.year, self.month, self.day)

                # It turns out you can't pass tzinfo directly, See
                # http://pythonhosted.org/pytz/
                # This lets us compute a UTC timestamp
                self.midnight_ts = timezone('US/Eastern').\
                                     localize(naive_dt).\
                                     timestamp()

                # This lets us parse the first line to initialize our
                # various attributes
                yield

                if self.do_process_chunk:
                    for chunk in self.chunks(self.numlines, infile):
                        yield self.process_chunk(chunk)
                else:
                    yield from self.chunks(self.numlines, infile)

    def process_chunk(self, all_bytes):
        '''Convert the structured ndarray `all_bytes` to the target_dtype

        If you did not specify do_process_chunk, you might run this yourself on
        chunks that you get from iteration.'''
        target_dtype = np.dtype(self.bytes_spec.target_dtype)
        combined = np.empty(all_bytes.shape, dtype=target_dtype)

        for name in self.bytes_spec.passthrough_strings:
            combined[name] = all_bytes[name]
        for name, dtype in self.bytes_spec.convert_dtype:
            curr = all_bytes[name]
            # .fromstring() converts bytes to integers, but needs
            # C-contiguous data, hence a .copy()
            # Also, 48 == ord(b'0'), subtracting from ANSI/ASCII/UTF8 yeilds
            # integer equivalents.
            # If generalizing this code, may want to do bounds checking
            a = np.fromstring(curr.copy(), np.uint8) - 48
            a = a.reshape((-1, curr.itemsize))

            # We now have an expanded decimal representation in a, we'll assign
            # appropriate multipliers for each place (e.g., (100, 10, 1))
            place_values = (10. ** np.arange(curr.itemsize - 1, 0 - 1, -1,
                                             dtype=dtype))

            # These don't have the decimal point in the TAQ file
            if name in ['Bid_Price', 'Ask_Price']:
                place_values /= 10000

            # And now we reduce to a more appropriate representation of a
            # single float or integer per number using matrix algebra
            combined[name] = a.dot(place_values)

        # Currently, there doesn't seem to be any value in converting to
        # numpy.datetime64, as PyTables wants float64's corresponding to the
        # POSIX Standard (relative to 1970-01-01, UTC) that it then converts to
        # a time64 struct on it's own

        # This math is probably a bit inefficient, but we have tested that it
        # works, and based on Dav's testing, this is taking negligible time
        # compared to the above conversions.
        time64ish = (self.midnight_ts +
                     combined['hour'] * 3600. +
                     combined['minute'] * 60. +
                     # I'm particularly amazed that this seems to work (in py3)
                     combined['msec'] / 1000.)

        # This is a vanilla float64 column, as time64 is a mess (e.g.
        # unsupported in HDF5)
        combined['Time'] = time64ish

        return combined

    def chunks(self, numlines, infile):
        '''Do the conversion of bytes to numpy "chunks"'''
        # TODO Should do check on numlines to make sure we get the right number

        if self.chunksize is None:
            self.chunksize = self.DEFAULT_CHUNKSIZE

        while(True):
            raw_bytes = infile.read(self.bytes_spec.bytes_per_line *
                                    self.chunksize)
            if not raw_bytes:
                break

            # This is a fix that @rdhyee made, but due to non-DRY appraoch, he
            # did not propagate his fix!
            numrows = len(raw_bytes) // self.bytes_spec.bytes_per_line
            all_bytes = np.ndarray(numrows, buffer=raw_bytes,
                                   dtype=self.bytes_spec.initial_dtype)

            yield all_bytes
