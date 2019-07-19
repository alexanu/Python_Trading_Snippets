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


