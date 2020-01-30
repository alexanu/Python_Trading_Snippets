'''Test the taq.processing module'''

import pytest
from pytest import mark


@mark.xfail
def test_split_chunks():
    raise NotImplementedError

if __name__ == '__main__':
    pytest.main("test_processing.py")
