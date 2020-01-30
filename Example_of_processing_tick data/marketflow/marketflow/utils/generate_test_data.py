#!/usr/bin/env python3

'''Mix up symbols and prices a little bit'''

from zipfile import ZipFile, ZIP_DEFLATED
from os import path

import marketflow
import marketflow.processing as tp


def main(fname_in, fname_out, size, frac):
    taq_in = marketflow.TAQ2Chunks(fname_in, do_process_chunk=False)
    downsampled = tp.Downsample(taq_in, frac)
    # We should downsample enough that things will fit in memory!
    recombined = tp.JoinedChunks(tp.SplitChunks(downsampled, 'Symbol_root'),
                                 'Symbol_root')
    sanitized = tp.Sanitizer(recombined)

    # Assemble our chunks - all of this should fit into memory for quick n'
    # easy testing
    write_len = 0
    chunks = []
    for chunk in sanitized:
        if len(chunk) + write_len > size:
            break
        chunks.append(chunk)
        write_len += len(chunk)

    # Compute a correct first line for this derived file
    line_len = len(taq_in.first_line)
    datestr, numlines = taq_in.first_line.split(b':')
    first_line = datestr + b':' + b' '*4 + str(write_len).encode()
    # padding for the rest of the line
    first_line += b' '*(line_len-len(first_line)-2) + b'\r\n'

    with open(fname_out, 'wb') as ofile:
        ofile.write(first_line)

        for chunk in sorted(chunks, key=lambda x: x[0]['Symbol_root']):
            ofile.write(chunk)

    basename = path.basename(fname_out)
    with ZipFile(fname_out + '.zip', 'w') as zf:
        zf.write(fname_out, basename, ZIP_DEFLATED)

    # Currently, the unzipped version of fname_out is left laying around!

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('fname_in', help="Path to Zipped TAQ data")
    parser.add_argument('fname_out', default='test_data_public',
                        help="Path to write output"
                             "(both zip archive and contained file")
    parser.add_argument('--size', type=int, default=10000,
                        help="Integer max of lines to sanitize and write")
    parser.add_argument('--frac', '-f', type=float, default=0.001,
                        help='Floating point probability'
                             'of returning each line')
    args = parser.parse_args()

    main(args.fname_in, args.fname_out, args.size, args.frac)
