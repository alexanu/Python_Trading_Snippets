install.packages("XML")
install.packages("stringr")
install.packages("RCurl")
install.packages("httr")
install.packages("data.table")

library(RCurl)
library(stringr)
library(XML)
library(httr)
library(curl)
library(data.table)


url<-"http://etfdb.com/screener/"
parsed_doc <- htmlParse(url)
tables <- readHTMLTable(parsed_doc,StringsAsFactors=FALSE)
NEW_ETFS <-as.data.table(as.matrix(tables[[1]]),
                         colClasses = c("character",
                                        "character",
                                        "numeric",
                                        "numeric",
                                        "numeric",
                                        "character"),
                         col=1:2,
                         StringsAsFactors=FALSE)
NEW_ETFS<-NULL
class(NEW_ETFS)
lapply(NEW_ETFS,class)
lapply(tables[[1]],class)

st
names(tables)

head(NEW_ETFS)









url <- "http://etf.finanztreff.de/etf_suche.htn?allocationId=&appropriationTypeCode=&etftyp=0&gearing=0&instrumentIdUnderlying=&issuerId=&quanto=0&regionId=&replicationMethod=SYNTHETIC&replicationMethod=COMPLETE&replicationMethod=OPTIMIZED&replicationMethod=SAMPLING&searchString=&sectorId=&securityTypeCode=&sektion=erweitert&suchflag=1"
parsed_doc <- htmlParse(url)





xpathSApply(parsed_doc, "//*[contains(text(), 'Gesellschaft')]")


parsed_doc

names(readHTMLTable(parsed_doc))
ETFS <- as.data.frame(readHTMLTable(parsed_doc))[3]
etf.list <- as.character(ETFS[,1])



ETF <- xpathSApply(parsed_doc, "//tr", xmlSize)

ETF2 <- xpathSApply(parsed_doc, "//tr", xmlGetAttr, "id")

ETF2 <- str_replace_all(ETF2, "etf_", "")
#print(ETF2)



installed.packages()
