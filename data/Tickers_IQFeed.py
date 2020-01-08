# Source: https://github.com/alexanu/atpy/blob/master/atpy/data/iqfeed/util.py



def get_symbols(symbols_file: str = None, flt: dict = None):
    """
    Get available symbols and information about them
    :param symbols_file: location of the symbols file (if None, the file is downloaded)
    :param flt: filter for the symbols
    """

    with tempfile.TemporaryDirectory() as td:
        if symbols_file is not None:
            logging.getLogger(__name__).info("Symbols: " + symbols_file)
            zipfile.ZipFile(symbols_file).extractall(td)
        else:
            with tempfile.TemporaryFile() as tf:
                logging.getLogger(__name__).info("Downloading symbol list... ")
                tf.write(requests.get('http://www.dtniq.com/product/mktsymbols_v2.zip', allow_redirects=True).content)
                zipfile.ZipFile(tf).extractall(td)

        with open(os.path.join(td, 'mktsymbols_v2.txt')) as f:
            content = f.readlines()

    logging.getLogger(__name__).debug("Filtering companies...")

    flt = {'SECURITY TYPE': 'EQUITY', 'EXCHANGE': {'NYSE', 'NASDAQ'}} if flt is None else flt

    cols = content[0].split('\t')
    positions = {cols.index(k): v if isinstance(v, set) else {v} for k, v in flt.items()}

    result = dict()
    for c in content[1:]:
        split = c.split('\t')
        if all([split[col] in positions[col] for col in positions]):
            result[split[0]] = {cols[i]: split[i] for i in range(1, len(cols))}

    logging.getLogger(__name__).debug("Done")

    return result
