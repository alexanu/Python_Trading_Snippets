
"""
You can download all tick data for given year. Data are downloaded for each month, extracted and concatenated into one CSV file.

python get_data_for_year_in_csv.py -u <truefxUsername> -p <truefxPassword> -f <folder> -y <year> -s <symbol>

There are some configuration variables in this script:

    -u or --username - username to login in TrueFX
    -p or --password - password to login in TrueFX
    -f or --folder - folder where to download data
    -y or --year - year for which to download data
    -s or --symbol - symbol for which to download data
"""


import cookielib
import urllib
import urllib2
import ssl
import os
import errno
from lxml import html
import url_provider
import zipfile
import glob
import sys, getopt


class UrlProvider:
    base_url_https = 'https://www.truefx.com/'
    base_url_http = 'http://www.truefx.com/'

    months = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December"
    }

    def get_login_url(self):
        return self.base_url_https + "?page=loginz"

    def get_download_referrer_url(self, year, month):
        return self.base_url_http \
                + "?page=download&description=" \
                + self.months.get(month).lower() + str(year) \
                + "&dir=" + str(year) + "/" \
                + self.months.get(month).upper() + "-" + str(year)

    def get_download_url_type_1(self, year, month, symbol):
        return self.base_url_http \
               + "dev/data/" \
               + str(year) + "/" \
               + str(year) + "-" + str(month).zfill(2) + "/" \
               + symbol.upper() + "-" + str(year) + "-" + str(month).zfill(2) + ".zip"

    def get_download_url_type_2(self, year, month, symbol):
        return self.base_url_http \
               + "dev/data/" \
               + str(year) + "/" \
               + self.months.get(month).upper() + "-" + str(year) + "/" \
               + symbol.upper() + "-" + str(year) + "-" + str(month).zfill(2) + ".zip"



    
class Manager:
    url_provider = url_provider.UrlProvider()
    login_response_cookies = False

    def download_and_merge_to_one_file(self, year, symbol, directory):

        self.download_for_year(year, symbol, directory)
        self.unzip_for_year(year, symbol, directory)

        output_filename = symbol.lower() + '-' + str(year) + '.csv'
        files = self.get_filenames_to_merge(str(year), symbol, directory)

        with open(directory + output_filename, 'w') as outfile:
            for fname in files:
                with open(fname) as infile:
                    for line in infile:
                        outfile.write(line)
                    infile.close()
            outfile.close()

        for fname in self.get_filenames_to_delete(year, symbol, directory):
            os.unlink(fname)

    @staticmethod
    def get_filenames_to_merge(year, symbol, directory):
        return glob.glob(directory + symbol.upper() + '-' + str(year) + '-*.csv')

    @staticmethod
    def get_filenames_to_delete(year, symbol, directory):
        return glob.glob(directory + symbol.lower() + '-' + str(year) + '-*.zip') \
               + glob.glob(directory + symbol.upper() + '-' + str(year) + '-*.csv')

    def unzip_for_year(self, year, symbol, directory):

        for month in range(1, 12, 1):
            self.unzip_for_month(year, month, symbol, directory)

    def unzip_for_month(self, year, month, symbol, directory):

        filename = self.get_downloaded_filename(month, symbol, year)

        filename_with_directory = directory + filename

        zip_ref = zipfile.ZipFile(filename_with_directory, 'r')
        zip_ref.extractall(directory)
        zip_ref.close()

    def download_for_year(self, year, symbol, destination_directory):

        for month in range(1, 12, 1):

            try:
                self.download_for_month(year, month, symbol, destination_directory)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def download_for_month(self, year, month, symbol, destination_directory):

        filename = self.get_downloaded_filename(month, symbol, year)

        filename_with_directory = destination_directory + filename

        if os.path.isfile(destination_directory + filename):
            raise OSError(errno.EEXIST, "File '" + filename_with_directory + "' already exists",
                          filename_with_directory)

        opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(debuglevel=0),
            urllib2.HTTPCookieProcessor(self.login_response_cookies)
        )

        opener.addheaders += [("Referer", self.url_provider.get_download_referrer_url(year, month))]

        url_to_download = self.url_provider.get_download_url_type_1(year, month, symbol)
        self.print_download_status(filename_with_directory, url_to_download)

        try:
            self.save_data_from_url_to_file(filename_with_directory, opener, url_to_download)
        except urllib2.HTTPError:
            url_to_download = self.url_provider.get_download_url_type_2(year, month, symbol)
            self.print_download_status(filename_with_directory, url_to_download)

            try:
                self.save_data_from_url_to_file(filename_with_directory, opener, url_to_download)
            except urllib2.HTTPError as http_error:
                raise http_error

        return filename_with_directory

    @staticmethod
    def print_download_status(filename_with_directory, url_to_download):
        print("Downloading '%s' to '%s'" % (url_to_download, filename_with_directory))

    @staticmethod
    def save_data_from_url_to_file(filename_with_directory, opener, url_to_download):
        response = opener.open(url_to_download)
        f = open(filename_with_directory, "wb")
        f.write(response.read())
        f.close()

    @staticmethod
    def get_downloaded_filename(month, symbol, year):
        return symbol.lower() + "-" + str(year) + "-" + str(month).zfill(2) + ".zip"

    def login_to_true_fx(self, username, password):

        form_data = {
            'USERNAME': username,
            'PASSWORD': password
        }

        form_data_encoded = urllib.urlencode(form_data)
        self.login_response_cookies = cookielib.CookieJar()

        ctx = self.get_ssl_default_context()

        opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(context=ctx, debuglevel=0),
            urllib2.HTTPCookieProcessor(self.login_response_cookies)
        )

        response = opener.open(self.url_provider.get_login_url(), form_data_encoded)

        if self.check_login_from_response(response):
            return True
        else:
            return False

    @staticmethod
    def get_ssl_default_context():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @staticmethod
    def check_login_from_response(response):
        the_page = response.read()
        tree = html.fromstring(the_page)

        if tree.xpath('count(//*[@id="login-form"])') == 0:
            return True
        else:
            return False

