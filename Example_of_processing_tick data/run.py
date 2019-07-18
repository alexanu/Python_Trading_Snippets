import process

df = process.TaqDataFrame('../data/my_random_dataset.zip','qts').load()
df.to_pickle('../data/dataframe.p')
# print (df.df)

# df.featurize('BLUFEN')