
## About

TrueFX script written in Python is written in order to download Forex tick data from [TrueFX](http://www.truefx.com/) service.

## Usage

You can download all tick data for given year. Data are downloaded for each month, extracted and concatenated into one CSV file.
```
python get_data_for_year_in_csv.py -u <truefxUsername> -p <truefxPassword> -f <folder> -y <year> -s <symbol>
```

There are some configuration variables in this script:

- `-u` or `--username` - username to login in TrueFX 
- `-p` or `--password` - password to login in TrueFX
- `-f` or `--folder` - folder where to download data 
- `-y` or `--year` - year for which to download data
- `-s` or `--symbol` - symbol for which to download data

