import requests, re
from bs4 import BeautifulSoup

class FinViz:

    def __init__(self):
        request = requests.get('http://finviz.com/')
        soup = BeautifulSoup(request.text,'html5lib')
        self._html = soup
        #self._html = BeautifulSoup(requests.get('http://finviz.com/').text,'html5lib')
        self._data = list()

    def refresh(self):
        self.__reinitalize__()
    
    def _reinitialize(self):  
        r = requests.get('http://finviz.com')
        self._html = BeautifulSoup(r.text)

    def _parseColumnData(self, data):  
        ret_data = data.findChild()
        ret_data = list(ret_data.children)
        result = list()
        for idx in ret_data:
            try:
                result.append(self._parseText(idx.getText()))                
            except:
                pass
        return result  

    def _parseText(self, text):
      
        #use regex for faster parsing of text, searching for numbers and words
        #define regEx pattern
        #"find all alpha upper and lower words
        #+ one and unlimited timees
        #match words who may or may not have spcaes betweent hem and
        #are mixed caes zero to unliited times
        regExText = '[A-Z a-z]+'
        #match a number with at least 1 to unlimited length
        ##the number must have a period and 1 through 2 numbers after it
        #match fully if there is a '+' OR '-' ZERO or 1 times
        regExDigit = '(\+|-?\d+.\d{1,2})'
        #regExDigit = '(\d+.\d{1,2})'
        listText = re.findall(regExText, text)
        listDigit = re.findall(regExDigit, text)
        resultSet = {
                     'index':listText[0],
                     'price':listDigit[0],
                     'change':listDigit[1],
                     'volume':listDigit[2],
                     'signal':listText[1]
                     }
        #return the resulting dictionary
        return resultSet
    
    def getLeftColumn(self):     
        data = self._getMainColumnData(0)
        a = self._parseColumnData(data)
        return a
        
    def getRightColumn(self):
        data = self._getMainColumnData(1)
        a = self._parseColumnData(data)
        return a

    def _getMainColumnData(self,column):
        searchResult = self._html.findAll('table', {'class':'t-home-table'})
        return searchResult[column]
        
        
    def getTrends(self):
        left_col = self.getLeftColumn()
        right_col = self.getRightColumn()
        
        combined_dict = list()
        for i in left_col:
            combined_dict.append(i)
        for i in right_col:
            combined_dict.append(i)
            
        #print(combined_dict)
        #return_dict = {"right_column": right_col, "left_column":left_col}
        return combined_dict
   
def test():
    print("testing now")
    testObject = FinViz()

    data = testObject.getTrends()
    for i in data:
        print(i)
    
if __name__ == "__main__":
    test()
