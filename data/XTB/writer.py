from os.path import join

import settings


def write_tsv(data, symbol, indicator, period):
    file_name = join(settings.BASE_DIR, "data", "{0}__{1}__{2}.tsv".format(symbol, indicator, period))
    data["DATE"] = data.index
    data.to_csv(file_name, index=False, sep="\t", na_rep="", header=True, mode="w", decimal=".")
