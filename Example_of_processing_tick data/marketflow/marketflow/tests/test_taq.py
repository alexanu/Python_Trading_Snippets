from os import path, listdir
import marketflow
import arrow
import pytest
import configparser
from pytest import mark
from dateutil.tz import gettz

# XXX We should turn this into a set-up fixture
test_path = path.dirname(__file__)
sample_data_dir = path.join(test_path, '../test-data/')
config = configparser.ConfigParser()
config.read(path.join(test_path, 'test_taq.ini'))
# We simply throw away key names
DATA_FILES = [y for x, y in config.items('taq-data')]
chunksize = int(config['parameters']['chunksize'])

# We can set up some processing this way
# Docs here: http://pytest.org/latest/fixture.html

# tmpdir = py.path.local('test-dir/')


@mark.parametrize('fname', DATA_FILES)
def test_h5_files(fname, tmpdir):
    # XXX Update to be appropriate conversion to HDF5
    sample = marketflow.TAQ2Chunks(sample_data_dir + fname)

    # Use tmpdir

    # for i in range(len(DATA_FILES)):
    #     test_file = DATA_FILES[i]
    #     # Generate name for output file. Assumes filename of form
    #     # "EQY_US_ALL_BBO_YYYYMMDD.zip"
    #     out_name = test_file[15:23]
    #     sample = marketflow.TAQ2Chunks(test_file)

    #     # XXX use temp files / directories to store data
    #     # http://pytest.org/latest/tmpdir.html

    #     # empty hdf5 table?
    #     # h5_table = sample.setup_hdf5('sample')

    #     # empty hdf5 table?
    #     h5_table = sample.setup_hdf5('sample')

    #     h5_table.append(chunk)

    #     h5_table.close()

    #     return out_name  # or out_names ideally!

@mark.parametrize('fname', DATA_FILES)
def test_data_available(fname):
    '''Test that our sample data is present

    Ideally, data should be exactly the data also available on Box in the
    financial-data folder maintained by D-Lab. These data are copyrighted, so if
    you're not a member of the D-Lab, you'll likely need to arrange your own
    access!

    By default, we have also created a small fake TAQ data file, available
    here:

        http://j.mp/TAQ-small-test-data-public
    '''
    data_dir_contents = listdir(sample_data_dir)
    assert fname in data_dir_contents


def test_ini_row_value():
    '''Test values read explicitly from test_taq.ini'''
    sample = marketflow.TAQ2Chunks(sample_data_dir +
                                   config['taq-data']['std-test-file'],
                                   chunksize=chunksize)
    chunk = next(sample)
    row0 = chunk[0]
    test_values = config['std-test-row-values']

    assert float(test_values['time']) == row0['Time']
    assert int(test_values['hour']) == row0['hour']
    assert int(test_values['minute']) == row0['minute']
    assert int(test_values['msec']) == row0['msec']
    assert test_values['exchange'].encode('ascii') == row0['Exchange']
    assert test_values['symbol_root'].encode('ascii') == row0['Symbol_root']


@mark.parametrize('fname', DATA_FILES)
def test_row_values(fname, numlines=5):

    sample = marketflow.TAQ2Chunks(sample_data_dir + fname,
                                   chunksize=chunksize)
    chunk = next(sample)

    # Check len(chunk) == min(sample.chunksize, length of file)
    print (sample.numlines)
    assert len(chunk) == sample.chunksize

    # Use raw_taq to read in raw bytes
    chunk_unprocessed_gen = marketflow.TAQ2Chunks(sample_data_dir + fname, chunksize=numlines, do_process_chunk=False)
    chunk_processed_gen = marketflow.TAQ2Chunks(sample_data_dir + fname, chunksize=numlines, do_process_chunk=True)
    chunk = next(chunk_unprocessed_gen)
    chunk_proc = next(chunk_processed_gen)

    month, day, year = chunk_unprocessed_gen.month, chunk_unprocessed_gen.day, chunk_unprocessed_gen.year

    for i in range(chunk.shape[0]):
        entry = chunk[i]
        msec = int(entry['msec'][2:5])

        date_object = arrow.Arrow(year, month, day,
            hour=int(entry['hour']),
            minute=int(entry['minute']),
            second=int(entry['msec'][0:2]),
            tzinfo=gettz('America/New York'))

        unix_time = date_object.timestamp + msec/1000

        assert unix_time == chunk_proc[i]['Time']

        # in bytes
        symbol_root = entry['Symbol_root']
        symbol_suffix = entry['Symbol_suffix']
        bid_price = int(entry['Bid_Price'][0:7]) + int(entry['Bid_Price'][7:11])/10000
        bid_size = int(entry['Bid_Size'])
        ask_price = int(entry['Ask_Price'][0:7]) + int(entry['Ask_Price'][7:11])/10000
        ask_size = int(entry['Ask_Size'])

        # Add assert statements
        assert bid_price == chunk_proc[i][7]
        assert bid_size == chunk_proc[i][8]
        assert ask_price == chunk_proc[i][9]
        assert ask_size == chunk_proc[i][10]

@mark.parametrize('fname', DATA_FILES)
def test_statistics(fname):
    # np.average()
    assert 1


@mark.xfail
def test_hdf5_rows_match_input(fname, h5_files):
    # XXX h5 files will return a list of files it's created
    raise NotImplementedError


if __name__ == '__main__':
    pytest.main("test_taq.py")

    # To test individual tests, call it here

    # test_row_values('EQY_US_ALL_BBO_20140206.zip')
    # tmpdir = 'test_dir'
    # h5_files(tmpdir)
