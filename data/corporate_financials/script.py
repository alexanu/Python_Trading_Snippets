import good_morning as gm

fd = gm.FinancialsDownloader()
kr = gm.KeyRatiosDownloader()
# %%
symbol = 'AAPL'
kr_fins = fd.download(symbol)
kr_ratios = kr.download(symbol)

# %
# %
list(kr_fins['income_statement']['title'].values) + list(kr_fins['balance_sheet']['title'].values) + list(
    kr_fins['cash_flow']['title'].values)

# %%
yearEndMonth = kr_fins['fiscal_year_end']
folder = 'income_statement'
columns = kr_fins[folder]['title']
dataframeFormatted = kr_fins[folder].T
# %%
import pandas as pd

firstRow = 2
indexYears = list(dataframeFormatted.index[firstRow:])
newIndex = []
for period in indexYears:
    year = period.year
    month = yearEndMonth + 1
    day = 1

    while month > 12:
        month = month - 12
        year += 1

    object = pd.datetime(year=year, month=month, day=day)
    newIndex.append(object)

# %%
newDataframe = pd.DataFrame(dataframeFormatted.values[firstRow:, :], columns=columns, index=newIndex)

# %%
list(kr_fins['income_statement']['title'].values) + list(kr_fins['balance_sheet']['title'].values) + list(
    kr_fins['cash_flow']['title'].values)
