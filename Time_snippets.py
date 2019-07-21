from datetime import timezone, datetime

def utc_to_local(self, utc_dt):
    utc_dt = datetime.strptime(utc_dt, "%Y-%m-%d %H:%M:%S")
    local = utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return local.strftime("%Y-%m-%d %H:%M:%S")

def utc_to_local(utc_dt):
    local = utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return local.strftime("%Y-%m-%d %H:%M:%S")

utc_dt = datetime.strptime("2018-11-01 01:45:00", "%Y-%m-%d %H:%M:%S")
print(utc_to_local(utc_dt))

