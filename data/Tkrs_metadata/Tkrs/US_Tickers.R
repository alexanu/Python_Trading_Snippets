
# We create a dataframe of the urls:
BORSE <- c("NYSE","NASDAQ","AMEX")
urls <- c("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download",
          "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download",
          "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download")
BORSE_URL <- data.frame(BORSE, urls, stringsAsFactors=F)

<<<<<<< HEAD
summary(datatb)
=======
#---------------------------------------------------------------------------------------------------------------------------

Y <- read.csv("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download")
urls[1]
>>>>>>> b3fca39db039cfd058202e73a71732f7e4933601

# Then we go through every of urls and receive a combine list of all tickers on 3 exchanges
install.packages("data.table")

library(stringr)
library(dplyr)
library(data.table)
datalist<-NULL # clear datalist (just in case)
datatb<-NULL
datalist = lapply(BORSE_URL$urls, #creates list
                  FUN=fread, # fread from the package "DATA.TABLE" does it much faster than read.table 
                  header=TRUE)
datalist <- mapply(cbind, datalist , "Borse"=BORSE_URL$BORSE , SIMPLIFY=F) # add new column to every data.table in the list and fills it with the symbol
datatb = rbindlist(datalist, # merging rows of 3 lists in one
		   fill=TRUE) # we need "Fill=True" as there are some columns which appear only in 1 database
datatb[Borse=="NASDAQ",industry:=Industry] # there are 2 industry columns, named differently. We need to merge them.
setnames(datatb, make.names(colnames(datatb))) # this remove spaces from column names

datatb<- datatb[,list(Symbol,Name,Sector,industry, Summary.Quote,Borse)] # keeping only needed columns
datatb[,Summary.Quote:=str_replace(Summary.Quote,"https://www.nasdaq.com/symbol/","")] # xxx


datatb[,.(.N),by=.(Summary.Quote, Symbol)]
max(datatb[,.(.N),by=.(Summary.Quote, Symbol)]$N)

datatb[,.(.N),by=.(Summary.Quote, industry)]
max(datatb[,.(.N),by=.(Summary.Quote)]$N)




