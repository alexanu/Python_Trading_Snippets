import datetime
import requests

class TDException(Exception):
    def __init__(self, error):
        self.error = error
    def __str__(self):
        return self.error

error_400 = TDException('Bad request')
error_401 = TDException('Unauthorized')
error_404 = TDException('Treasury data not found')
error_429 = TDException('Too many requests')
error_500 = TDException('Internal server error')
error_503 = TDException('Service unavailable')

class TreasuryDirect(object):
    def __init__(self):
        self.base_url = 'https://www.treasurydirect.gov'
        self.securities_endpoint = '/TA_WS/securities/'
        self.debt_endpoint = '/NP_WS/debt/'

    def _raise_status(self, response):
        if response.status_code == 400:
            raise error_400
        elif response.status_code == 401:
            raise error_401
        elif response.status_code == 404:
            raise error_404
        elif response.status_code == 429:
            raise error_429
        elif response.status_code == 500:
            raise error_500
        elif response.status_code == 503:
            raise error_503
        else:
            response.raise_for_status()

    def _check_cusip(self, cusip):
        if len(cusip) != 9:
            raise Exception('CUSIP is not length 9')

    def _check_date(self, date, dt_format):
        if isinstance(date, str):
            try:
                datetime.datetime.strptime(date, dt_format)
            except ValueError:
                raise ValueError('Incorrect data format, should be ' + dt_format)
            return date
        if isinstance(date, datetime.date):
            return date.strftime(dt_format)

    def _check_type(self, s):
        types = ['Bill', 'Note', 'Bond', 'CMB', 'TIPS', 'FRN']
        if s in types:
            return
        else:
            raise ValueError('Incorrect security type format, should be one of (Bill, Note, Bond, CMB, TIPS, FRN)')

    def _process_request(self, url):
        r = requests.get(url)
        self._raise_status(r)
        try:
            d = r.json()
            return d
        except:
            # No data - Bad Issue Date
            return None

    def security_info(self, cusip, issue_date):
        """
        This function returns data about a specific security identified by CUSIP and issue date.
        """
        self._check_cusip(cusip)
        issue_date = self._check_date(issue_date, '%m/%d/%Y')
        url = self.base_url + self.securities_endpoint + '{}/{}?format=json'.format(cusip, issue_date)
        security_dict = self._process_request(url)
        return security_dict

    def security_hist(self, security_type, auction=False, days=7, pagesize=2, reopening='Yes'):
        """
        This function returns data about announced or auctioned securities.  
        Max 250 results.  
        Ordered by announcement date (descending), auction date (descending), issue date (descending), security term length (ascending)
        If auction is true returns auctioned securities
        """
        self._check_type(security_type)
        if auction:
            s = 'auctioned'
        else:
            s = 'announced'
        url = self.base_url + self.securities_endpoint + s + '?format=json' + '&type={}'.format(security_type) 
        announced_dict = self._process_request(url)
        return announced_dict

    def security_type(self, security_type):
        """
        This function returns data about securities of a particular type.
        """
        self._check_type(security_type)
        url = self.base_url + self.securities_endpoint + '{}?format=json'.format(security_type)
        security_dict = self._process_request(url)
        return security_dict

    def security_search(self):
        raise NotImplementedError('Not implemented yet')

    def current_debt(self):
        """
        This function returns the most recent debt data.
        """
        url = self.base_url + self.debt_endpoint + 'current?format=json'
        debt = self._process_request(url)
        return debt

    def get_debt_by_date(self, dt):
        """
        This function returns the debt data for a particular date.
        """
        dt = self._check_date(dt, '%Y/%m/%d')
        url = self.base_url + self.debt_endpoint + '{}?format=json'.format(dt)
        debt = self._process_request(url)
        return debt

    def get_debt_range(self, start_dt, end_dt):
        """
        This function returns debt data based on the parameters passed.  
        """
        start_dt = self._check_date(start_dt, '%Y-%m-%d') 
        end_dt = self._check_date(end_dt, '%Y-%m-%d')
        url = self.base_url + self.debt_endpoint + 'search?startdate={}&enddate={}&format=json'.format(start_dt, end_dt)
        debt = self._process_request(url)
        return debt

if __name__=='__main__':
    td = TreasuryDirect()
    print td.security_info('912796CJ6', '02/11/2014') 
    print td.security_info('912796AW9', datetime.date(2013, 3, 7))
    print td.security_hist('FRN')
    print td.security_hist('FRN', True)
    print td.security_type('FRN')
    # print td.security_search()
    print td.current_debt()
    print td.get_debt_by_date(datetime.date(2014, 1, 2))
    print td.get_debt_range(datetime.date(2014, 1, 1), datetime.date(2014, 2, 1))
