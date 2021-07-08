import pandas as pd


class Permno_Map(object):
    """1. Reads in dsenames file from crsp
       2. Subsets """

    def __init__(self, dsefile='crsp/dsenames.csv'):
        self.dsenames = pd.read_csv(dsefile)
        # Once everything is working, perhaps make this automatic.
        # For now, it's easier to debug with smaller functions run one at a
        # time.
        # self.process(dsenames)
        self.nasdaq = dsenames.PRIMEXCH == "Q"

    def get_permno(self, cd, root, date):
        '''Get the permno for a given symbol root.

        Remember, permno does not change with suffix.'''
        that_permno = cd[cd.SYM_ROOT == root]
        permno_on_date = that_permno.loc[(cd.BEGDATE <= date) 
                                         & (cd.ENDDATE >= date)
                                         , 'PERMNO'] 
        if len(permno_on_date > 0):
            return permno_on_date
        else:
            raise NotImplementedError

    def process(self, dsenames):
        '''Run all processing steps in a reasonable order'''
        dsenames = self.dse_subset(dsenames)
        dsenames = self.dse_rootsplit(dsenames)
        dsenames = self.drop_dups(dsenames)

        self.clean_dsenames = dsenames

    def dse_subset(self, dsenames, date=20100101, regular=True,
                   active=True, beneficial=False, when_issued=False):
        '''Limit to our "good" set of securities.
           Default settings include securites that are actively trading in normal
           fashion on some exchange
        date : int
            Not really an int, but the naïve conversion from the datestring.
        regular : bool
            Limit to "regular" stocks. i.e. the security is past the "When-Issued" 
            stage and the company is not going through bankruptcy proceedings)
        active : bool
            Limit to entries for stocks that are actively trading
        beneficial : bool
            If =False, we exclude stocks that are "shares of beneficial interest", 
            which indicates that the stocks are not trading normally due to their
            inclusion in some sort of trust. 
        when_issued : bool
            If =False, we exclude when_issued shares, which have been approved for 
            trading but have not yet begun trading actively
        '''
        bad_permnos = [14209, 15141, 91845]
        # Here, we index out any securities whose records don't make sense
        dsenames = dsenames[~dsenames['PERMNO'].isin(bad_permnos)]
        # By default, we only include securities that were trading at some
        # point from January 2010 onwards
        dsenames = dsenames[dsenames['NAMEENDT'] >= date]
        if regular:
            # SECSTAT == "R" indicates that the security is "Regular" (i.e. the
            # security is past the "When-Issued" stage and the company is not
            # going through bankruptcy proceedings)
            dsenames = dsenames[dsenames['SECSTAT'] == "R"]
        if active:
            # TRDSTAT == "A" indicates that the security is actively trading
            dsenames = dsenames[dsenames['TRDSTAT'] == "A"]

        dsenames['SYMBOL_LEN'] = dsenames['TSYMBOL'].str.len()
        dsenames['LAST'] = dsenames['TSYMBOL'].str[-1]
        dsenames['LAST2'] = dsenames['TSYMBOL'].str[-2:]
        
        # The 'S' suffix indicates the "Shares of Beneficial Interest, which do
        # not trade like regular securities The 'V' and 'WI' suffixes indicate
        # "When-Issued" shares, which have been authorized to trade, but have
        # not actually begun trading
        # XXX Maybe make this attribute defined at a "higher level"?
        if beneficial == False:
            ben = (dsenames['LAST'] == "S") & (dsenames['SYMBOL_LEN'] == 5)
            dsename = dsenames[~ben]
        if when_issued == False:
            whenissued_nasdaq = ((dsenames['LAST'] == "V") &
                             (dsenames['SYMBOL_LEN'] == 5) & self.nasdaq)
            whenissued_nonnasdaq = ((dsenames['LAST2'] == "WI") &
                                (dsenames['SYMBOL_LEN'] > 3) & ~self.nasdaq)
            dsenames = dsenames[~(whenissued_nasdaq & whenissued_nonnasdaq)]
        return dsenames

    def dse_rootsplit(self, dsenames):
        '''Splits the root and the suffix into two separate variables,
        SYM_ROOT and SYM_SUFFIX and flags suffix extraction cases
        FLAG index:
            =0 : base case, symbol has no suffix
            =1 : NASDAQ, share class
            =2 : NASDAQ, foreign shares or voting/non-voting shares
            =3 : NASDAQ, reverse stock split
            =4 : non-NASDAQ, share class suffix
        Includes manual adjustments for idiosyncratic securities, should
        be re-evaluated from time to time
        '''
        # Flag = 0 is our base case (i.e. the ticker symbol has no suffix)
        dsenames['FLAG'] = 0
        # When the ticker has no suffix, the root is just the ticker symbol, and the
        # suffix is an empty string
        dsenames['SYM_ROOT'] = dsenames['TSYMBOL']
        dsenames['SYM_SUFFIX'] = ""
        dsenames['TICKER_LEN'] = dsenames['TICKER'].str.len()

        class_equal_last = dsenames.SHRCLS == dsenames.LAST

        # nasdaq_long true for NASDAQ securities with a ticker symbol
        # longer than 4 characters. 4 is the maximum number of characters for
        # a ticker symbol on the NASDAQ.
        nasdaq_long = self.nasdaq & (dsenames.SYMBOL_LEN > 4)

        # flag1 denotes securities with share class suffixes,
        # e.g. a company on the NASDAQ that has Class A and Class B shares

        flag1 = nasdaq_long & class_equal_last

        # flag2 denotes two cases:
        # - Suffixes Y and F denote shares in foreign companies
        # - Suffixes J and K denote voting and non-voting shares, respectively
        flag2 = ~flag1 & nasdaq_long & dsenames.LAST.isin(["Y", "J", "F", "K"])

        # flag3 denotes stocks going through reverse stock split
        # these securities keep this ticker symbol for ~3 weeks post-split
        flag3 = ~flag1 & nasdaq_long & (dsenames.LAST == "D")

        # flag4 denotes non-NASDAQ stocks w/ share class suffix
        flag4 = ~self.nasdaq & (dsenames.SYMBOL_LEN > 3) & class_equal_last

        # There is a fifth set of suffixed ticker symbols that do not fit into
        # the above categories, but they do have a unifying manual adjustment.
        # We denote this set as "funny" (not "funny" ha ha).

        funny_permnos = [85254, 29938, 29946, 93093, 92118, 83275, 82924,
                         82932, 77158, 46950, 90655]

        funny = (dsenames.PERMNO.isin(funny_permnos) &
                 (dsenames.SYMBOL_LEN - dsenames.TICKER_LEN == 1) &
                 dsenames.LAST.isin(["A", "B", "C", "S"])
                 )

        dsenames.loc[flag4, "FLAG"] = 4
        dsenames.loc[flag3, "FLAG"] = 3
        dsenames.loc[flag2, "FLAG"] = 2
        dsenames.loc[flag1, "FLAG"] = 1

        # Here, we group together the symboled suffixes to make the final
        # root-suffix separation cleaner. `sym5_with_suffix` is the set of
        # special cases with more than 4 characters in the symbol
        sym5_with_suffix = flag1 | flag2 | flag3
        symbol_with_suffix = flag4 | funny | sym5_with_suffix

        # Finally, the big enchilada, the separation of each ticker symbol into
        # its root and its symbol. Since we are only dealing with suffixes of
        # length 1, the root will consist of all but the last character, and
        # the root will be the ticker symbol's last character

        dsenames.loc[symbol_with_suffix, "SYM_ROOT"] = \
            dsenames.loc[symbol_with_suffix, "TSYMBOL"].str[0:-1]
        dsenames.loc[symbol_with_suffix, "SYM_SUFFIX"] = \
            dsenames.loc[symbol_with_suffix, "TSYMBOL"].str[-1]

        # There were a few wonky observations, so we do some additional manual
        # adjustments

        dsenames.loc[dsenames.PERMNO == 14461, "SYM_ROOT"] = \
            dsenames.loc[dsenames.PERMNO == 14461, "TSYMBOL"].str[0:-1]
        dsenames.loc[dsenames.PERMNO == 14461, "SYM_SUFFIX"] = \
            dsenames.loc[dsenames.PERMNO == 14461, "TSYMBOL"].str[-1]
        dsenames.loc[dsenames.PERMNO == 13914, "SYM_ROOT"] = \
            dsenames.loc[dsenames.PERMNO == 13914, "TSYMBOL"]
        dsenames.loc[dsenames.PERMNO == 13914, "SYM_SUFFIX"] = ""
        dsenames.loc[dsenames.PERMNO == 92895, "SYM_ROOT"] = "SAPX"
        dsenames.loc[dsenames.PERMNO == 92895, "SYM_SUFFIX"] = ""

        return dsenames

    def drop_dups(self, dsenames):
        '''Consolidates multiple records for same ticker symbol into one
        by collapsing trading date range
        '''
        # Finally, we want to ensure that, when the same information is
        # recorded, the date range listed for the record reflects the entire
        # range over which the security was actively trading.

        # For instance, if a security stopped trading for a six month period,
        # it has two entries in this file. We want both of those entries to
        # include beginning date for the security's trading before the six
        # month break and the end date for the security's trading after the six
        # month break.

        # To do this, we first want to reset the index in the dsenames dataframe
        dsenames = dsenames.reset_index(drop=True)

        # When we say that we want to adjust the dates 'when the same
        # information is recorded,' we make that adjustment based on the
        # following seven variables in the data frame:
        # - Permno, the two components of the ticker symbol, the name of the
        # company the CUSIP number (current and historical), and
        # - the primary exchange on which the security trades

        # We first create a new data frame sorted on these 7 columns, which
        # only includes said 7 columns

        levels_sort = ['PERMNO', 'SYM_ROOT', 'SYM_SUFFIX', 'COMNAM', 'CUSIP',
                       'NCUSIP', 'PRIMEXCH']
        dsenames_sort = dsenames.sort_values(by=levels_sort).loc[:, levels_sort]
        dsenames = dsenames.sort_values(by=levels_sort)

        # We create two new variables, begdate and enddate, to capture the full
        # range of dates for which each security trades. The default case, when
        # a security only matches with itself based on the 7 sort levels, is
        # that the beginning date is the same as the beginning effective name
        # date, and the end date is the same as the end effective name date.

        dsenames['BEGDATE'] = dsenames.NAMEDT
        dsenames['ENDDATE'] = dsenames.NAMEENDT

        # We create a new dataframe that only includes the sort variables
        dsenames_sort_squish = dsenames_sort.loc[:, levels_sort]

        # Here, we create two copies of the dataframe:
        # 1. One without the first record, and
        # 2. one without the last
        dsenames_nofirst = dsenames_sort_squish.iloc[1:].reset_index(drop=True)
        dsenames_nolast = dsenames_sort_squish.iloc[:-1].reset_index(drop=True)

        # We then create a boolean matrix based on whether the entries of each
        # matrix match
        compare_matrix = (dsenames_nofirst == dsenames_nolast)

        # If the i-th record matches the next record for all 7 variables, then
        # the i-th row of the compare matrix will be all true. We extract the
        # index for subsetting purposes
        same_as_below = compare_matrix.all(axis=1)
        same_as_below_index = same_as_below.index[same_as_below]

        # In order to collapse the end dates, we will also need an index to
        # indicate if a record is the same as the one above.  This is simply
        # caputured by adding 1 to the first index we found
        same_as_above_index = same_as_below_index + 1

        # Finally, we loop through the first Int64Index we found to bring the
        # earliest `BEGDATE` for a record down to all of its matches.  Doing
        # this matching iteratively mitigates the issue of a particular
        # security having more than 2 records match based on the 7 variables.
        for i in same_as_above_index:
            dsenames['BEGDATE'].iat[i] = dsenames['BEGDATE'].iat[i-1]

        # Similar logic is used to bring the latest ENDDATE up - we just do it
        # backwards
        for i in same_as_below_index[::-1]:
            dsenames['ENDDATE'].iat[i] = dsenames['ENDDATE'].iat[i+1]

        # Finally, we output a final dataframe that includes only the columns
        # we sorted on and our new date variables. Since the same information
        # is recorded for these files now, we drop the duplicates
        final_columns = levels_sort + ['BEGDATE', 'ENDDATE']

        return dsenames.drop_duplicates(subset=final_columns).loc[:, final_columns]
