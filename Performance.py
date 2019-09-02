# Source: https://github.com/thoriuchi0531/adagio

from enum import Enum
import pandas as pd


class PerfStats(Enum):
    ANN_RETURN = "annualised return"
    ANN_VOL = "annualised vol"
    SHARPE = "Sharpe ratio"
    MAX_DD = "max drawdown"
    CALMAR = "Calmar ratio"
    SKEWNESS = "skewness"
    KURTOSIS = "kurtosis"


class Performance(object):
    def __init__(self, data, ann_factor=252):
        self.data = pd.DataFrame(data)
        self.ann_factor = ann_factor

    def annualised_return(self):
        return self._fmt_result(self._ann_return(),
                                PerfStats.ANN_RETURN.value)

    def annualised_vol(self):
        return self._fmt_result(self._ann_vol(), PerfStats.ANN_VOL.value)

    def sharpe(self):
        result = self._ann_return().div(self._ann_vol())
        return self._fmt_result(result, PerfStats.SHARPE.value)

    def drawdown(self):
        return self.data.div(self.data.cummax()).sub(1)

    def max_drawdown(self):
        return self._fmt_result(self._max_dd(), PerfStats.MAX_DD.value)

    def calmar(self):
        result = self._ann_return().div(self._max_dd())
        return self._fmt_result(result, PerfStats.CALMAR.value)

    def skewness(self):
        result = self.data.pct_change().skew()
        return self._fmt_result(result, PerfStats.SKEWNESS.value)

    def kurtosis(self):
        result = self.data.pct_change().kurt()
        return self._fmt_result(result, PerfStats.KURTOSIS.value)

    def summary(self):
        funcs = [self.annualised_return, self.annualised_vol,
                 self.sharpe, self.max_drawdown, self.calmar,
                 self.skewness, self.kurtosis]
        return pd.concat([f() for f in funcs], axis=0)

    # def _clean_summary(self, summary):
    #     return summary.style.format({
    #         PerfStats.ANN_RETURN.value: "{:.2%}",
    #         PerfStats.ANN_VOL.value: "{:.2%}",
    #         PerfStats.SHARPE.value: "{:.2}",
    #         PerfStats.MAX_DD.value: "{:.2%}",
    #         PerfStats.CALMAR.value: "{:.2}",
    #         PerfStats.SKEWNESS.value: "{:.2}",
    #         PerfStats.KURTOSIS.value: "{:.2}"
    #     })

    def _ann_return(self):
        return self.data.pct_change().mean().mul(self.ann_factor)

    def _ann_vol(self):
        return self.data.pct_change().std().mul(self.ann_factor ** 0.5)

    def _max_dd(self):
        return self.drawdown().min().mul(-1)

    def _fmt_result(self, result, name):
return result.to_frame(name).T
