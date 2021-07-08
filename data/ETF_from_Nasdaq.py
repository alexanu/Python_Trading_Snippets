# Source: https://github.com/lwittchen/tricia

import time
import logging
from scipy.optimize import minimize
import pandas as pd
import pandas_datareader.data as web
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
import numpy as np
from tqdm import tqdm

import matplotlib.pyplot as plt


def get_return(weights: pd.Series, mean_rets: pd.Series) -> float:
    """
    Total Portfolio Return given the respective weights
    """
    return (weights * mean_rets).sum() * 252


def get_std(weights: pd.Series, cov_matrix: pd.DataFrame) -> float:
    """
    Portfolio Variance given the respective covariance Matrix
    """
    return np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights))) * np.sqrt(252)


def get_sharpe_ratio(pf_return: float, pf_std: float) -> float:
    """
    Sharpe ratio assuming risk-free rate is zero
    """
    return pf_return / pf_std


def clean_colnames(data: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure that all column names are lowercase and don't contain spaces
    """
    clean_names = {x: x.lower().replace(" ", "_") for x in data.columns}
    return data.rename(columns=clean_names)


def get_symbols_from_nasdaq() -> pd.DataFrame:
    """
    Load symbols of all stocks traded on NASDAQ 
    Text file provided by NASDAQ, available on their FTP server
    Updated daily 
    """
    # load text file from NASDAQs FTP server
    nasdaq_symbols_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
    all_stock_symbols = pd.read_csv(nasdaq_symbols_url, sep="|")

    # adjust column names
    clean_stock_symbols = clean_colnames(all_stock_symbols)
    return clean_stock_symbols


def load_etf_data(symbols: list, with_sleep: bool = False) -> dict:
    etfs = {}
    for symbol in tqdm(symbols):
        logging.info(f"Load data for {symbol}")
        try:
            data = web.DataReader(
                name=symbol,
                data_source="yahoo",
                start=start_time,
                end=end_time,
                # api_key=cfg.av_key,
            )
            etfs[symbol] = clean_colnames(data)
            if with_sleep:
                time.sleep(5)
        except Exception as e:
            logging.warning(
                f"Error while loading data for {symbol} from Alpha Vantage. Error: {e}",
                exc_info=True,
            )
    return etfs


if __name__ == "__main__":

    # inputs
    start_time = pd.Timestamp(2019, 1, 1)
    end_time = pd.to_datetime("now") + pd.DateOffset(days=0, normalize=True)
    N = 10
    k = 10000

    # get etf universe (all ETFs traded on NASDAQ)
    stock_symbols = get_nasdaq_symbols()
    etf_universe = stock_symbols.query("ETF")  # implicit query: ETF==True
    etf_symbols = etf_universe.index.tolist()

    # load data
    random_symbols = np.random.choice(etf_symbols, 20)
    etf_dict = load_etf_data(symbols=random_symbols, with_sleep=True)

    # extract adjusted close and volume data
    etfs = pd.DataFrame()
    for symbol, data in etf_dict.items():
        temp_data = data[["adj_close", "volume"]]
        temp_data["symbol"] = symbol
        etfs = etfs.append(temp_data)

    # extract etf with largest traded volume
    largest_vol_symbols = etfs.groupby("symbol")["volume"].mean().nlargest(N).index
    largest_etfs = etfs[etfs["symbol"].isin(largest_vol_symbols)]

    # change from long to wide format
    largest_etfs_wide = largest_etfs.pivot(values="adj_close", columns="symbol")

    # calc percentage returns
    pct_rets = np.log(largest_etfs_wide / largest_etfs_wide.shift(1))

    # calc total returns and covariance
    mean_rets = pct_rets.mean()
    cov = pct_rets.cov()

    print('ETF Universe:')
    print(mean_rets * 252)

    # find the optimal long-only portfolio by
    # searching for the max the sharpe ratio in our etf universe

    # with scipy
    bnds = [(0, 1) for _ in range(N)]
    cons = [{"type": "eq", "fun": lambda x: np.sum(x) - 1}]
    x0 = [1 / N for _ in range(N)]

    def obj_func(weights):
        global cov
        global pf_std
        pf_std = get_std(weights=weights, cov_matrix=cov)
        pf_ret = get_return(weights=weights, mean_rets=mean_rets)
        sr = pf.get_sharpe_ratio(pf_return=pf_ret, pf_std=pf_std)
        return -sr

    res = minimize(fun=obj_func, x0=x0, bounds=bnds, constraints=cons)
    print(f"Optimal Portfolio weights by optimization:")
    print(pd.Series(res.x, index=mean_rets.index).round(2))
    print(f"Sharpe: {-res.fun}")
    print(f"Return: {get_return(res.x, mean_rets)}")
    print(f"Std: {get_std(res.x, cov)}")

    # using Monte Carlo
    results = []
    for _ in tqdm(range(k)):
        rand_nums = np.random.random(size=len(mean_rets))
        rand_weights = rand_nums / rand_nums.sum()

        pf_ret = get_return(weights=rand_weights, mean_rets=mean_rets)
        pf_std = get_std(weights=rand_weights, cov_matrix=cov)
        results.append(dict(zip(mean_rets.index, rand_weights), ret=pf_ret, std=pf_std))

    results_df = pd.DataFrame(results)
    results_df["sharpe"] = results_df["ret"] / results_df["std"]
    max_sharpe_idx = results_df["sharpe"].idxmax()
    min_std_idx = results_df["std"].idxmin()

    # plot the results
    results_df.plot.scatter(x="std", y="ret")
    plt.plot(
        results_df.loc[max_sharpe_idx, "std"],
        results_df.loc[max_sharpe_idx, "ret"],
        color="red",
        marker="*",
    )
    plt.show()

    print(f"Optimal Portfolio by monte carlo:")
    print(results_df.loc[max_sharpe_idx])
