import urllib2
from urllib2 import urlopen
from bs4 import BeautifulSoup
from Tkinter import *
import xlsxwriter
import xlrd
import os
import stopwatch
from tkFileDialog import askopenfilename  

#Author Justin DoBosh <justin.dobosh@spartans.ut.edu>
master = Tk()
#Need to open the excel file first, because if we open it inside the allETFInfo class it only writes the last etf entered
workbookToWriteTo = xlsxwriter.Workbook('ETFRatings.xlsx')
worksheetToWriteTo  = workbookToWriteTo.add_worksheet()
"""
Class Level Variables:
	self.master
	self.etfsGUIInput
	self.rootURLNum
	self.GUIETFList
	self.rootURLStr
	self.etfSymbol 
	self.row 
	self.baseURL 
	self.soup
	self.ETFInfoToWrite
"""
#Run these ticker symbols for maxfund ---> CNSAX,ANZAX,FISCX,FACVX,PACIX,VCVSX,DEEAX,ACCBX,CLDAX

#------------------------------------ GUI ------------------------------------------------------------------------------------------
class GUI:

	def __init__(self):
		self.master = master

	def init_window(self):
		self.master.title("ETF Data Scraper")
		directionsText = StringVar()
		DirectionLabel = Label(master, textvariable=directionsText, font = "Arial 16 " )
		directionsText.set("Select the web site you want to get data from. Then upload the excel file that contains \n the ticker symbols you want data for.")
		DirectionLabel.config(background="#C1CDCD")
		DirectionLabel.pack( padx=10, pady=15, fill=X)

		#self.etfsGUIInput = StringVar()
		#etfEntry = Entry(master, textvariable=self.etfsGUIInput, font = "Arial 14 bold")	

		self.rootURLNum = IntVar()
		etfRadioBtn = Radiobutton(master, text="etf.com", variable=self.rootURLNum, value=1, font = "Arial 14 bold ")
		etfRadioBtn.config(background="#C1CDCD")
		etfRadioBtn.pack(  padx=5, pady=5)

		maxFundsRadioBtn = Radiobutton(master, text="maxfunds.com", variable=self.rootURLNum, value=2, font = "Arial 14 bold ")
		maxFundsRadioBtn.config(background="#C1CDCD")
		maxFundsRadioBtn.pack( padx=5, pady=5)

		smartMoneyRadioBtn = Radiobutton(master, text="Smartmoney.com", variable=self.rootURLNum, value=3, font = "Arial 14 bold ")
		smartMoneyRadioBtn.config(background="#C1CDCD")
		smartMoneyRadioBtn.pack( padx=5, pady=10)

		def cleanAndReturnListofEtfs():
			fundList = []
			name = askopenfilename(filetypes = ( ("Excel", "*.xlsx" ), ("All files", "*.*")))
			workbook = xlrd.open_workbook(name)
			worksheet = workbook.sheet_by_name('Sheet1')
			num_rows = worksheet.nrows - 1
			num_cells = worksheet.ncols - 1
			curr_row = -1
			while curr_row < num_rows:
				curr_row += 1
				row = worksheet.row(curr_row)
				#print 'Row:', curr_row
				curr_cell = -1
				while curr_cell < num_cells:
					curr_cell += 1
					# Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
					cell_type = worksheet.cell_type(curr_row, curr_cell)
					cell_value = worksheet.cell_value(curr_row, curr_cell)
					cell_value = str(cell_value)
					fundList.append(cell_value)
					
			self.rootURLStr = StringVar()

			self.rootURLNum = self.rootURLNum.get()
			if(self.rootURLNum == 1):
				self.rootURLStr = "http://www.etf.com/"
			elif(self.rootURLNum == 2):
				self.rootURLStr = "http://www.maxfunds.com/funds/data.php?ticker="
			elif(self.rootURLNum == 3):
				self.rootURLStr = "http://www.marketwatch.com/investing/Fund/"
			
			row = 0
			t = stopwatch.Timer()

			for etfSymbol in fundList:
				row += 1
				#self.progress.set(etfList.index(etfSymbol))
				myEtf = ETFDataCollector(etfSymbol, row, self.rootURLStr)
				myEtf.parseTargetWebPage()
				#use an if statement to find out which website we are scraping
 				if(self.rootURLStr == "http://www.etf.com/"):
 					try:
 						myEtf.etfDotComInfo()
 					except:
 						e = sys.exc_info()[0]
 						self.ETFInfoToWrite = [etfSymbol]
 						ETFInfoToWrite = self.ETFInfoToWrite
 						excel = excelSetup(ETFInfoToWrite,row)
						excel.etfInfoSetup()
 						e = ""
 					else:
 						pass
 				elif(self.rootURLStr == "http://www.maxfunds.com/funds/data.php?ticker="):
 					try:
 						myEtf.maxfundsDotComInfo()
 					except:
 						e = sys.exc_info()[0]
 						self.ETFInfoToWrite = [etfSymbol]
 						ETFInfoToWrite = self.ETFInfoToWrite
 						excel = excelSetup(ETFInfoToWrite,row)
						excel.maxfundsSetup()
 						e = ""
 					else:
 						pass
 				elif(self.rootURLStr == "http://www.marketwatch.com/investing/Fund/"):
					try:
 						myEtf.smartmoneyDotComInfo()
 					except:
 						e = sys.exc_info()[0]
 						self.ETFInfoToWrite = [etfSymbol]
 						ETFInfoToWrite = self.ETFInfoToWrite
 						excel = excelSetup(ETFInfoToWrite,row)
						excel.smartmoneySetup()
 						e = ""
 					else:
 						pass
			t.stop()
			print str(t.elapsed) + " elapsed time"
			#close the window 
			master.destroy()

		etfSubmitBtn = Button(master, text="Choose File", command=cleanAndReturnListofEtfs, font = "Arial 16 ")
		etfSubmitBtn.pack(padx=5, pady=10)
		etfSubmitBtn.config(highlightbackground="#C1CDCD")
           	
#------------------------------------ ETFDataCollector ------------------------------------------------------------------------------------------
class ETFDataCollector:
	def __init__(self, etfSymbol, row, baseURL):
		self.etfSymbol = etfSymbol
		self.row = row 
		self.baseURL = baseURL
		self.ETFInfoToWrite = []

	def parseTargetWebPage(self):
		try:
			website = urllib2.urlopen(self.baseURL + self.etfSymbol)
			sourceCode = website.read()
			self.soup = BeautifulSoup(sourceCode)
		except:
			e = sys.exc_info()[0]
			print self.etfSymbol + " Cannot Be Found while parsing " + str(e)
			e = ""
		else:
			pass

	def etfDotComInfo(self):
		#Test funds: spy,qqq,vti,ivv,GLD,VOO,EEM
		row = self.row

		#parse document to find etf name 
		etfName = self.soup.find('h1', class_="etf")
		#extract etfName contents (etfTicker & etfLongName)
		etfTicker = etfName.contents[0]
		etfLongName = etfName.contents[1]
		etfTicker = str(etfTicker)
		etfLongName = etfLongName.text
		etfLongName = str(etfLongName)

		#get the time stamp for the data scraped 
		etfInfoTimeStamp = self.soup.find('div', class_="footNote")
		dataTimeStamp = etfInfoTimeStamp.contents[1]
		formatedTimeStamp =  'As of ' + dataTimeStamp.text
		formatedTimeStamp = str(formatedTimeStamp)

		#create vars 
		etfScores = []
		cleanEtfScoreList = []
		#parse document to find all divs with the class score
		etfScores = self.soup.find_all('div', class_="score")
		#loop through etfScores to clean them and add them to the cleanedEtfScoreList
		for etfScore in etfScores:
			strippedEtfScore = etfScore.string.extract()
			strippedEtfScore = str(strippedEtfScore)
			cleanEtfScoreList.append(strippedEtfScore)
		#turn cleanedEtfScoreList into a dictionary for easier access
		
		self.ETFInfoToWrite = [etfTicker, etfLongName, formatedTimeStamp, int(cleanEtfScoreList[0]), int(cleanEtfScoreList[1]), int(cleanEtfScoreList[2])]
		ETFInfoToWrite = self.ETFInfoToWrite
		excel = excelSetup(ETFInfoToWrite,row)
		excel.etfInfoSetup()
		

	def maxfundsDotComInfo(self):
		#Test funds: VTIAX,PTTRX,PRFDX,DBLTX,TGBAX,FCNTX,CNSAX,ANZAX,FISCX,FACVX,PACIX,VCVSX,DEEAX,ACCBX,CLDAX
		row = self.row
 		#get ETFs name
 		etfName = self.soup.find('div', class_="dataTop")
 		etfName = self.soup.find('h2')
 		etfName = str(etfName.text)
 		endIndex = etfName.find('(')
 		endIndex = int(endIndex)
 		fullEtfName = etfName[0:endIndex]
 		startIndex = endIndex + 1
 		startIndex = int(startIndex)
 		lastIndex = etfName.find(')')
 		lastIndex = int(lastIndex)
 		lastIndex = lastIndex - 1
 		tickerSymbol = etfName[startIndex: lastIndex]
 		#get ETFs Max rating score
 		etfMaxRating = self.soup.find('span', class_="maxrating")
 		etfMaxRating = str(etfMaxRating.text)

 		#create array to store name and rating 
 		self.ETFInfoToWrite = [fullEtfName, tickerSymbol, int(etfMaxRating)]
 		ETFInfoToWrite = self.ETFInfoToWrite
 		excel = excelSetup(ETFInfoToWrite,row)
		excel.maxfundsSetup()

	def smartmoneyDotComInfo(self):
		#Test funds: OAKLX,OAKGX,OARMX,OAKBX,OAKIX,OARIX
		row = self.row
		
 		#get etf Name
 		etfName = self.soup.find('h1', id="instrumentname")
 		etfName = str(etfName.text)
 		#get etf Ticker
 		etfTicker = self.soup.find('p', id="instrumentticker")
 		etfTicker = str(etfTicker.text)
 		etfTicker = etfTicker.strip()


 		self.ETFInfoToWrite.append(etfName)
 		self.ETFInfoToWrite.append(etfTicker)

 		#get Lipper scores ***NEEDS REFACTORING***
 		lipperScores = self.soup.find('div', 'lipperleader')
 		lipperScores = str(lipperScores)
 		lipperScores = lipperScores.split('/>')
 		for lipperScore in lipperScores:
 			startIndex = lipperScore.find('alt="')
 			startIndex = int(startIndex)
 			endIndex = lipperScore.find('src="')
 			endIndex = int(endIndex)
 			lipperScore = lipperScore[startIndex:endIndex]
 			startIndex2 = lipperScore.find('="')
 			startIndex2 = startIndex2 + 2
 			endIndex2 = lipperScore.find('" ')
 			lipperScore = lipperScore[startIndex2:endIndex2]
 			seperatorIndex = lipperScore.find(':')
 			endIndex3 = seperatorIndex
 			startIndex3 = seperatorIndex + 1

 			lipperScoreNumber = lipperScore[startIndex3:]
 			if lipperScoreNumber == '' and lipperScoreNumber == '':
 				pass
 			else:
 				self.ETFInfoToWrite.append(int(lipperScoreNumber))

 		ETFInfoToWrite = self.ETFInfoToWrite
 		excel = excelSetup(ETFInfoToWrite,row)
		excel.smartmoneySetup()

#------------------------------------ excelSetup ------------------------------------------------------------------------------------------
class excelSetup:
	def __init__(self,ETFInfoToWrite,row):
		self.ETFInfoToWrite = ETFInfoToWrite
		self.row = row		

	def etfInfoSetup(self):
		# Widen the first column to make the text clearer.
		worksheetToWriteTo.set_column('A:F', 30)
		#Add formating
		format = workbookToWriteTo.add_format()
		format.set_text_wrap()
		format.set_font_size(14)
		format.set_font_name('Arial')
		format.set_align('center')
		# Write some data headers.
 		worksheetToWriteTo.write('A1', 'Ticker Symbol', format)
 		worksheetToWriteTo.write('B1', 'Fund Name', format)
 		worksheetToWriteTo.write('C1', 'Time Stamp', format)
 		worksheetToWriteTo.write('D1', 'Efficiency', format)
 		worksheetToWriteTo.write('E1', 'Tradability', format)
 		worksheetToWriteTo.write('F1', 'Fit', format)
 		# Start from the first cell below the headers.
 		row = self.row
 		col = 0

 		for etf in self.ETFInfoToWrite:
			worksheetToWriteTo.write(self.row, col, etf, format)
			col += 1
		col = 0

	def maxfundsSetup(self):
		# Widen the first column to make the text clearer.
		worksheetToWriteTo.set_column('A:C', 40)
		#Add formating
		format = workbookToWriteTo.add_format()
		format.set_text_wrap()
		format.set_font_size(14)
		format.set_font_name('Arial')
		format.set_align('center')
		# Write some data headers.
 		worksheetToWriteTo.write('A1', 'ETF Name', format)
 		worksheetToWriteTo.write('B1', 'Ticker Symbol', format)
 		worksheetToWriteTo.write('C1', 'Max Rating', format)
 		# Start from the first cell below the headers.
 		row = self.row
 		col = 0

 		for etf in self.ETFInfoToWrite:
			worksheetToWriteTo.write(self.row, col, etf, format)
			col += 1
		col = 0

	def smartmoneySetup(self):
		# Widen the first column to make the text clearer.
		worksheetToWriteTo.set_column('A:G', 30)
		#Add formating
		format = workbookToWriteTo.add_format()
		format.set_text_wrap()
		format.set_font_size(14)
		format.set_font_name('Arial')
		format.set_align('center')
		# Write some data headers.
		worksheetToWriteTo.write('A1', 'Fund Name', format)
 		worksheetToWriteTo.write('B1', 'Ticker Symbol', format)
 		worksheetToWriteTo.write('C1', 'Total Return', format)
 		worksheetToWriteTo.write('D1', 'Consistent Return', format)
 		worksheetToWriteTo.write('E1', 'Preservation', format)
 		worksheetToWriteTo.write('F1', 'Tax Efficiency', format)
 		worksheetToWriteTo.write('G1', 'Expense', format)
 		# Start from the first cell below the headers.
 		row = self.row
 		col = 0

 		for etf in self.ETFInfoToWrite:
			worksheetToWriteTo.write(self.row, col, etf, format)
			col += 1
		col = 0
#------------------------------------ CallToGo ------------------------------------------------------------------------------------------
#Starts the application 
def callToGo():
	#Sets the height and width of the window
	master.geometry("900x300") 
	master.configure(background='#C1CDCD')
	#Inits the application 
	app = GUI()
	#sets up the Tkinter window
	app.init_window()
	#Starts Tkinter
	master.mainloop()
	#close the workbook after the all the data is pulled and written to the excel file
	workbookToWriteTo.close()
	#opens the excel file (tested on mac, but not on windows)
	os.system("open ETFRatings.xlsx")


callToGo()