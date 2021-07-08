'''This script is based on the ITCH v5.0 spec.

It assumes their squirrelly binary message format.'''

import struct
import gzip

from .utility import ManyWriters


class ITCHv5:
    '''Convert an ITCH v5.0 file to reasonable records

    Currently, we redundantly unpack and keep/print the record type indicator

    Struct documented here:
    https://docs.python.org/3.4/library/struct.html#format-characters

    ITCH v5.0 record formats detailed in section 4.1 here:
    http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

    I'm using the equivalent p/s codes to indicate unevaluated bytes vs actual
    ascii / "alpha" data

    Using ord here to make lookups more straightforward with bytes (not sure
    why rec[0] below doesn't return a byte...)
    '''

    std_prefix = '>c2h6p'
    rec_types = {ord('S'): std_prefix + 'c',              # System Event Message
                 ord('R'): std_prefix + '8s2ci2c2s5cic',  # Stock Directory Message
                 ord('H'): std_prefix + '8s2c4s',         # Stock Trading Action Message
                 ord('Y'): std_prefix + '8sc',            # Reg SHO Restriction
                 ord('L'): std_prefix + '4s8s3c',         # Market Participation Position Message
                 ord('V'): std_prefix + '3q',             # MWCB Decline Level Message
                 ord('W'): std_prefix + 'c',              # MWCB Breach Message
                 ord('K'): std_prefix + '8sicl',          # IPO Quoting Period Update
                 ord('A'): std_prefix + 'qci8sl',         # Add Order Message
                 ord('F'): std_prefix + 'qci8sl4s',       # Add Order (MPID) Message
                 ord('E'): std_prefix + 'qlq',            # Order Executed Message
                 ord('C'): std_prefix + 'qlqcl',          # Order Executed (with Price) Message
                 ord('X'): std_prefix + 'ql',             # Order Cancel Message
                 ord('D'): std_prefix + 'q',              # Order Delete Message
                 ord('U'): std_prefix + '2q2l',           # Order Replace Message
                 ord('P'): std_prefix + 'qcl8slq',        # Trade Message
                 ord('Q'): std_prefix + 'q8slqc',         # Cross Trade Message
                 ord('B'): std_prefix + 'q',              # Broken Trade Message
                 ord('I'): std_prefix + '2qc8s3l2c'       # NOII Message
                 }

    def __init__(self, fname):
        self.fname = fname

    def records(self):
        '''Generator that returns unpacked records from self.fname

        Returns records as a list. Note that ascii strings are returned as
        bytes.
        '''
        with gzip.GzipFile(self.fname) as infile:
            while(True):
                # XXX A different idea (that would make it easier to use numpy)
                # would be to read 3 bytes, and also get the record type in
                # this initial read.
                rec_len, = struct.unpack('>h', infile.read(2))
                # .read() just returns '' when it's at EOF
                if rec_len == '':
                    break

                rec = infile.read(rec_len)

                try:
                    fmt = self.rec_types[rec[0]]
                    unpacked_rec = list( struct.unpack(fmt, rec) )

                    # This unpacks that annoying 6-byte int timestamp that
                    # struct can't handle (maybe we should be using numpy?)
                    unpacked_rec[3] = int.from_bytes(unpacked_rec[3], 'big')

                    yield unpacked_rec

                except KeyError:
                    # Silently ignore unknown record types
                    # (at least until we have them all)
                    print('Unkown record type: ' + chr(rec[0]))
                    pass

    def to_string(self, b):
        '''Try to decode b to ascii

        This is why people don't like Python 3
        '''
        try:
            return b.decode('ascii')
        except AttributeError:
            return str(b)

    def print_records(self):
        '''This could be redefined to get what kind of output you like'''
        # XXX This is rather insane - converting from bytes to text
        # so that SAS can parse the text back into bytes
        for rec in self.records():
            # Note - we have to do this after unpacking the above
            # 6-byte integer
            print(','.join(self.to_string(r) for r in rec))

    def to_csvs(self):
        '''Output the records to a CSV file for each message type'''
        base, _ = self.fname.rsplit('-')
        with ManyWriters(base) as writers:
            for rec in self.records():
                rec = [self.to_string(r) for r in rec]
                writer = writers.get_writer(rec[0])
                writer.writerow(rec[1:])


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    # parser.add_argument('--overwrite', action='store_true',
    #                     help='Overwrite existing CSV files')
    parser.add_argument('fnames', nargs='+', metavar='filename',
                        help='A v5.0 ITCH file to convert')
    parsed = parser.parse_args()

    for name in parsed.fnames:
        print('converting {} to multiple CSV files'.format(name))

        # This logic is harder given that we convert to multiple CSV files

        # if (not parsed.overwrite) and path.exists(some_outname):
        #     print('skipping, {} exists'.format(some_outname))
        # else:

        itch = ITCHv5(name)
        itch.to_csvs()
        # or use `.to_records()`
