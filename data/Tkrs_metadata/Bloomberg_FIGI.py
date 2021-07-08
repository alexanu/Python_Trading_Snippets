class FigiApi(object):

    base_url = 'https://api.openfigi.com/v1/mapping'
    api_key = "471197f1-50fe-429b-9e11-a6828980e213"

    @classmethod
    def retrieve_mapping(cls, id_type=None, ids=None, exch_codes=None):
        req_data = list()
        for i in range(0, len(ids)):
            req_data.append({"idType": id_type,
                             "idValue": ids[i],
                             "exchCode": exch_codes[i]})
        r = requests.post(cls.base_url,
                          headers={"Content-Type": "text/json",
                                   "X-OPENFIGI-APIKEY": cls.api_key},
                          json=req_data)
        return r
