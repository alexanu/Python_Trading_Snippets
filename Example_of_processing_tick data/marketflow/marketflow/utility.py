'''A set of utilities for financial data analysis'''

import csv
from time import time

# Keeping track of a bundle of output files for, e.g., ITCH data


class ManyWriters:
    '''Keep track of a set of files and formatters around a given base'''

    writers = {}
    open_files = []

    def __init__(self, basename):
        self.basename = basename

    def __enter__(self):
        return self

    def create_writer(self, rec_type):
        '''Create a new writer, and store it in the writers dict

        rec_type : str
            Will be combined with self.basename to determine filename
        '''
        outname = self.basename + '_' + rec_type + '.csv'
        # csv.writer docs specify newline=''
        outfile = open(outname, 'w', newline='')
        self.open_files.append(outfile)

        return csv.writer(outfile)

    def get_writer(self, rec_type):
        '''Get a writer for the specified rec_type, creating if needed

        rec_type : str
            Will be combined with self.basename to determine filename
        '''
        if rec_type not in self.writers:
            self.writers[rec_type] = self.create_writer(rec_type)

        return self.writers[rec_type]

    def close_files(self):
        for f in self.open_files:
            f.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_files()


# Benchmarking


def timeit(method):
    '''Return a function that behaves the same, except it prints timing stats.

    Lightly modified from Andreas Jung. Unlicensed, but simple enough it should
    not be a license issue:
        https://www.andreas-jung.com/contents/a-python-decorator-for-measuring-the-execution-time-of-methods
    '''
    def timed(*args, **kw):
        tstart = time()
        result = method(*args, **kw)
        tend = time()

        print('{} {!r}, {!r}: {:.3} sec'.format(
            method.__name__, args, kw, tend-tstart))
        return result

    return timed
