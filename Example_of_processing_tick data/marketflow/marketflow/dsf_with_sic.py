class DSF_SIC_Map(object):
    """docstring for SIC_Map"""
    def __init__(self, dsffile = 'crsp/dsf.csv', sicfile = 'sic_codes.txt'):
        self.dsf = pd.read_csv("dsf.csv", dtype = {'CUSIP': np.str, 'PRC': np.float}, na_values = {'PRC': '-'})
        self.sic = pd.read_table(sicfile, header = 1)
        self.sic.columns = ['HSICCD', 'SICNAME']
    def process(self, day = 20100101, columns = ['PERMNO', 'DATE', 'PRC', 'VOL', 'SHROUT', 'RET', 'HSICCD']):
        self.dsf_startdate(date = day)
        self.dsf_subset(to_keep = columns)
        self.sic_merge()
    def dsf_startdate(self, date = 20100101):
        self.dsf = self.dsf[self.dsf.DATE >= date]
    def dsf_subset(self, to_keep = ['PERMNO', 'DATE', 'PRC', 'VOL', 'SHROUT', 'RET', 'HSICCD']):
        self.dsf = self.dsf[to_keep]
    def sic_merge(self):
        self.clean_dsf = self.dsf.merge(self.sic, how = "left")