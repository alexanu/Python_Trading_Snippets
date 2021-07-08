
library()
install.packages(c("readr","dplyr","xlsx","quantmod","devtools")) # when running on the RStudio Cloud some needed packages are already pre-installed

library(readr)
library(dplyr)
library(data.table)
library(stringr)
library(xlsx)
library(magrittr)
library(quantmod)
library(devtools)
install_github("gregce/ipify") # this package is from GitHub
library(ipify) # needed to clarify the IP


if_else(# the function is from dplyr. There is also base::ifelse()
        ipify::get_ip()=="77.189.111.53", # this is my home IP
        Nasdaq<- fread( # the fread is faster, however it doesn't work in the office because of firewall
                        "https://indexes.nasdaqomx.com//docs//AppendixNQGIIndexes.csv",# the list contains more than 16ths                                                                                           indeces  
                        showProgress = FALSE),  # hides messages
        Nasdaq <- read.csv("https://indexes.nasdaqomx.com//docs//AppendixNQGIIndexes.csv",  
                           stringsAsFactors = FALSE) %>% 
                  as.data.table())  # the transformation into data.table 


Nasdaq2 <- Nasdaq
Nasdaq <-NULL
Nasdaq2 <-NULL
names(Nasdaq2)<-make.names(names(Nasdaq2))
head(Nasdaq2)
head(Nasdaq)


############ cleaning the list of indexes #####################################################################
names(Nasdaq)<-make.names(names(Nasdaq)) # puting dot instead of spaces into the column names
Nasdaq<- Nasdaq[Currency=="EUR", # concentrate only on indexes in EUR (Around 2600 indeces)      
		            list(Price.Return.Symbol,Index.Name, Geography)] # keeping only needed columns
Nasdaq<- Nasdaq[grep("[0-9]{4}", Price.Return.Symbol)] # leaving only rows, which contain 4-digit code, i.e specific sector
Nasdaq<-Nasdaq[!grepl("LMEUR", Price.Return.Symbol),] # leaving only rows, which don't have "LM" in the name, i.e. no large, med or small capitalization 
Nasdaq<-Nasdaq[!grepl(" Cap ", Index.Name),] # If there is "Cap" in the name, it means the index is focused on certain capitalization. We do not need that => need to exclude
Nasdaq[,Sector.Code:=str_extract(Price.Return.Symbol,"[[:digit:]]{4}")] # new column with 4-digit code sector code
Nasdaq[,Industry.Code:=str_c(str_sub(Sector.Code,1,1),"000")] # for creation of separate column for the broad sector, we need to extract 1st digit from sector and add "000" to it
Nasdaq[,Industry.Code:=str_replace(Industry.Code,"0000","0001")] # Oil & Gas has code 0001
Nasdaq<- Nasdaq[Sector.Code==Industry.Code,] # we will concentrate only on the index for the whole industry, not sub-sectors
Nasdaq[,Sector.Code:=as.integer(Sector.Code),]

############# getting the countries & industry information ####################################################

# The file is taken in Dec 2017 from
# http://www.ftserussell.com/files/support-documents/icb-rgs-structural-conversion-map

id <- "16R4aKHBrmykKpE59Mrt5gClkykJuwTBJ" 
ICB <- read.csv(sprintf("https://docs.google.com/uc?id=%s&export=download",
                         id),   
                sep=";",   
                stringsAsFactors = FALSE) %>%  
       as.data.table()  # the data.table::fread function didn't work in the office, ...
			 	# ... hence transformation into data.table is done in 2 steps  

Industry <- ICB[,.(.N),by=.(Old.ICB.Industry.code, Old.ICB.Industry)]


id_country <- "1EPWUelh1WHwWTlzWAvj-3C_mfdx_dQmZ"
Countries <- read.csv(sprintf("https://docs.google.com/uc?id=%s&export=download",
                              id_country),   
                      sep=";",   
                      stringsAsFactors = FALSE,   
                      skip=1) %>%  
             as.data.table() # the data.table::fread function didn't work in the office, ...
				     # ... hence transformation into data.table is done in 2 steps

############ vlookuping from the google drive tables #########################################################

setkey(Nasdaq,Sector.Code)
setkey(Industry,Old.ICB.Industry.code)
    Nasdaq<- Nasdaq[Industry,nomatch=0]

setkey(Nasdaq,Geography)
setkey(Countries,Nasdaq.Country.Code)
    Nasdaq<- Nasdaq[Countries,nomatch=0]

# Could be done like this:
# Nasdaq[Geography %in% Countries$Nasdaq.Country.Code,]
# ... but no additional columns appear. It is just filtering

#################################################################################################################

Nasdaq[,':='(Geography=NULL,Industry.Code=NULL,N=NULL,Name=NULL,ETF.REGION=NULL)] # delete not needed column

max(Nasdaq[,.N,by=.(Country,Sector.Code)]$N) # Check if every combination of Industry-Country appears only once

Test_Tickers <- Nasdaq[sample(1:nrow(Nasdaq), 10, replace=T),1] %>% # we select 10 random tickers from our list
		    unlist() %>% as.character()


End_Date = Sys.Date()-3 # format should be “2015-01-19” # ther URL contains end and start dates
Start_Date = End_Date-60 #format should be “2015-01-19” # ther URL contains end and start dates

Path_Work <-"L:/AGCS/CFO/Metadata/For 2013/Weight table/Nasdaq/"

unlink(paste0(Path_Work,"/*")) # deleting all the previous files

lapply(Test_Tickers, # for all selected tickers we apply the function "download.file"
	function(x) download.file( # we need this function because fread and other functions didn't work in the office
					   sprintf("https://indexes.nasdaqomx.com/Index/ExportHistory/%s?startDate=%sT00:00:00.000&endDate=%sT00:00:00.000&timeOfDay=EOD.xlsx",
							x,Start_Date, End_Date), # the function puts the variables in every place instead of "%s"
					   destfile = paste0(Path_Work,x,".xlsx"),
					   mode="wb")) # this mode is needed

# I need to try to upload files to gdrive or dropbox and read from there

file_list <- list.files(path=Path_Work, pattern="*.xlsx") # create list of all .xlsx files in folder

data <-  do.call("rbind", # like this we stack the data from all downloaded files into 1 data.frame (1 block under another)
	           lapply(file_list, # we avoiding loops like this
	                  function(x) cbind( # we need this function in order to add the new column 
                                           read.xlsx(paste0(Path_Work, x), #fread didn'T work for these files
                                                     endRow=as.integer(End_Date-Start_Date)-1, # the file is not clean, hence fread doesn't work
                                                     sheetIndex=1, # the fomula requires to specify the sheet
								     colIndex = c(1, 2)), # we need only 2 columns
							Price.Return.Symbol=str_sub(x,1,-6)) # this the new column we are adding: ...
							# it is the name of the index (we cut away the ".xlsx" of the file names)
				)
	          ) %>% as.data.table # transform to data.table

return_duration <- c(1,5,22,130) # different durations of returns in days
returns_data <- rbindlist( # puts every block one under another
			        lapply(return_duration, # to every return duration applies the formuls from below... 
	   			  	   function(x) cbind( # this function creates several columns...
								   Date=data[,Trade.Date],
							     	   Price.Return.Symbol=data[,Price.Return.Symbol],
   							         Index.Value=data[,Index.Value],
					     		         as.data.table(
										 	ROC(data[,Index.Value], # adding the return column...
										     	    x) #... of different duration
								   			),
								   Return_dur=paste0(x,"-days_ret")) # adding a column of type of return
					    )
				  )



setkey(returns_data,Price.Return.Symbol)
setkey(Nasdaq,Price.Return.Symbol)
final_data <- merge(Nasdaq,returns_data, all.y=TRUE) # adding index information to the return data

report_date <- data[,max(Trade.Date, na.rm=TRUE),]


