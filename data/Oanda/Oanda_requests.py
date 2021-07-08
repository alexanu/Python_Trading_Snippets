
# schema is here: 
# https://bubbl.us/NDc3NDc4NC80MDkzODQwL2U4Mjg0YTdlMmMzOTBlY2E5MzIyYzZjNGEzMjVhNGUw@X?utm_source=shared-link&utm_medium=link&s=10055319

import json
import requests
import datetime

gran_times = {"M1": 5000 // 1440, "M4": 5000 // 360, "M5": 5000 // 288, "M10": 5000 // 144,
              "M15": 5000 // 96, "M30": 5000 // 48, "H1": 5000 // 24, "H2": 5000 // 12,
              "H3": 5000 // 8, "H4": 5000 // 6, "H6": 5000 // 4, "H8": 5000 // 3, "H12": 5000 / 2,
}

def createQueryC(count, granularity):
    query = {
        "granularity": granularity,
        "count": count
    }
    return query

def createQueryT(fromTime, toTime, granularity):
    query = {
        "granularity": granularity,
        "from": fromTime,
        "to": toTime
    }
    return query

def getData(suppINTD, granularity, instrument, count=5000):
    with open("acctdetails") as file:
        acctdet = json.load(file)

    if suppINTD:
        toTime = datetime.datetime.utcnow()
        toTime = toTime.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            fromTime = toTime - datetime.timedelta(days=gran_times[granularity])

            fromTime = str(fromTime.isoformat("T")) + "Z"
            toTime = str(toTime.isoformat("T")) + "Z"
            query = createQueryT(fromTime, toTime, granularity)
        except:
            query = createQueryC( count, granularity)

    else:
        query = createQueryC( count, granularity)
