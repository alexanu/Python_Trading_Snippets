# Status as of 29th of Dec 2016
# Content:
#	0) Check package "TAWNY"
#	1) DJIA components. Status: OK
#	2) SP500 components. Status: OK
#	3) NSDQ100 components. Status: OK
#	4) SP100 components. Status: OK
#	5) SPDR components. Status: OK
#	6) iShares components. Status: UNDER DEVELOPMENT
#	7) PowerShares components with qmao package


Check package "TAWNY"
require(tawny)
indx <- getIndexComposition('^DJI')
for(stk in indx) { getSymbols(stk) }



DJ_components <- function(){ 
		install.packages("XML")
		library(XML) #Load package. WE need readHTMLTable function from this package
				
		url = readLines('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average#Components')
		all <- readHTMLTable(url,
				     header=TRUE, # header=True to have clear column names
				     stringsAsFactors=FALSE) # insert "stringsAsFactors" already in readHTMLTable to avoid factors
		DJIA <- as.data.frame(all[2]) # The current components are in the 2nd table
		names(DJIA)<-gsub("NULL.", "", names(DJIA )) # we need to remove "NULL." from the column names
		DJIA<- subset(DJIA, select = c(Symbol,Company,Date.Added)) # leaving only needed columns
		DJIA_Components <- DJIA[,"Symbol"] # The symbols are in the 1st column
		DJIA_Components} # The prolongation of this function could be to run it periodically to see the change in components

sp500_components <- function(){ 
      				install.packages("XML")
      				library(XML) #Load package. We need readHTMLTable function from this package
  
				url = readLines('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
				all <- readHTMLTable(url,header=TRUE,stringsAsFactors=FALSE) # We nee to insert "stringsAsFactors" already in readHTMLTable to avoid factors. We need to say header=True in to have clear column names.
				SP500 <-as.data.frame(all[1]) # the URL contains 2 tables. The current components are in the 1st table. The 2nd table contains the changes.
				names(SP500)<-gsub("NULL.", "", names(SP500)) # we need to remove "NULL." from the column names
				SP500<- subset(SP500, select = c(Ticker.symbol,Security,GICS.Sector,GICS.Sub.Industry)) # leaving only needed columns
				SP500_Components <- SP500[,"Ticker.symbol"] # The symbols are in the 1st column
				SP500_Components} # The prolongation of this function could be to run it periodically to see the change in components

nsdq100_components <- function(){
				url = 'http://www.nasdaq.com/quotes/nasdaq-100-stocks.aspx?render=download'
				temp = read.csv(url, header=TRUE, stringsAsFactors=F)  
				tickers = temp[, 'Symbol']
				return(tickers)

sp100_components <- function(){
      				install.packages("XML")
      				library(XML) #Load package. We need readHTMLTable function from this package
  
      				
      				url = readLines('http://www.cboe.com/products/indexcomponents.aspx')
				all <- as.data.frame(readHTMLTable(url,header=TRUE,stringsAsFactors=FALSE)) 
				tickers <- all[,"NULL.TICKER"] # the tickers are located in the certain column
				return(tickers)} # The prolongation of this function could be to run it periodically to see the change in components



# Constituetes of SPDRs -------------------------------------------------------------------------------------------------------
library(data.table)
spdr = unlist( # strsplit result the list, but we need character => we need unlist
		strsplit("XLY,XLP,XLE,XLF,XLV,XLI,XLB,XLK,XLU", split=",")) 
url <- paste0("http://www.sectorspdr.com/sectorspdr/IDCO.Client.Spdrs.Holdings/Export/ExportCsv?symbol=",
               spdr) # paste0("a", "b") == paste("a", "b", sep="")
datalist<-NULL # clear datalist (just in case)
datalist = lapply(url, #creates list of dataframes assuming tab separated values with a header   
                  FUN=fread, # fread from the package "DATA.TABLE" does it much faster than read.table (standard functionality). The comparison of time could be done using function system.time(x), where x is our expression
		  skip=1, # we need to start from the 2nd row, as there is some crap in the 1st row
                  select =1:3) # we need only first 3 columns
datalist <- mapply(cbind, # add new column ...
		   datalist, # ...to every data.table in the list ...
		   "SPDR"=spdr ,  # ... and fills it with the symbol
		   SIMPLIFY=F) 
datatb = rbindlist(datalist) # merging rows
setnames(datatb, make.names(colnames(datatb))) # this remove spaces from column names
datatb[, Index.Weight:= as.numeric(gsub("%","",Index.Weight))/100] # the weight are somehow not regognized as numeric (probably because of the % sign). I delete "%" and then everything is fine.

# Some additional steps for analysis
datatb[,sum(Index.Weight), by=SPDR] # check if the weight add up to 100%
datatb[SPDR=="XLP"] # shows all rows related to 1 SPDR
datatb[,.(Num_of_members= length(Symbol), # returns the number of members...
          CR3=sum(head(Index.Weight,3)), # ...share of TOP 3 holdings...
          CR10=sum(head(Index.Weight,10))), # ...share of TOP 10 holdings...
       by=SPDR] # ... for every ticker




# Get any ishares Components ----------------------------------------------------------------------------------------
# The link below leads to the location of all 300-350 products of IShares. There is the excel:
https://www.ishares.com/us/products/etf-product-list#
#The link below is the example of the link to download ETF constitutes. Quite complex.
https://www.ishares.com/us/products/239500/ishares-select-dividend-etf/1467271812596.ajax?fileType=csv&fileName=DVY_holdings&dataType=fund
# The code below is from SIT. The link is old => doesn't work.

us.ishares.components <- function(Symbol = 'DVY', date = NULL, debug = F){
				url = paste('http://us.ishares.com/product_info/fund/holdings/', 
					     Symbol, 
					     '.htm?periodCd=d', 
					     sep='')
				if( !is.null(date) ) url = paste('http://us.ishares.com/product_info/fund/holdings/', 
							    Symbol, 
							    '.htm?asofDt=', 
							    date.end(date), 
							    '&periodCd=m', sep='')
				txt = join(readLines(url))

				# extract date from this page
				temp = remove.tags(extract.token(txt, 'Holdings Detail', 'Holdings subject to change')) # these 2 functions are from SIT
				date = as.Date(spl(trim(temp),' ')[3], '%m/%d/%Y') # the function spl is from SIT
 
				# extract table from this page
				temp = extract.table.from.webpage (txt, 'Symbol', has.header = T) # the function  is from SIT
  
				colnames(temp) = trim(colnames(temp))
				temp = trim(temp)
				tickers = temp[, 'Symbol']
				keep.index = nchar(tickers)>1
				weights = as.double(temp[keep.index, '% Net Assets']) / 100
				tickers = tickers[keep.index]
    
				out = list(tickers = tickers, weights = weights, date = date)
				if(debug) out$txt = txt
				out}




# Get PowerShares Components ----------------------------------------------------------------------------------------
# There is "dlPowerShares" function in the qmao package. If the function is called with its defaults, ...
# ...it will download the PowerShares product list from http://www.invescopowershares.com/products/
# In the qmao package, dlPowerShares is used by getHoldings.powershares. 
# Also, getHoldings will call getHoldings.powershares if one of the Symbols passed to it is the symbol of a PowerShares ETF.

	require("qmao")
	Symbol <- "PGX"
	dat <- dlPowerShares(event.target = "ctl00$MainPageLeft$MainPageContent$ExportHoldings1$LinkButton1", # the first string ...
				# ... inside of the javascript:__doPostBack() function that you will get when you ...
				# ... right click the “Download” link and “Copy Link Address”.
                            action = paste0("holdings.aspx?ticker=", Symbol)) # the product-specific part of the action url
			    
# Internally, the code searches the source of the page for the values of the fields for the "aspnetForm". 
# It then uses those values in a call to postForm (from the RCurl package.)








