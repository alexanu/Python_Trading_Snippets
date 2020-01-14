# Source: https://github.com/jfouk/NewsParser/blob/master/wordSearch.py


from lxml import html
import requests
import threading
from subprocess import call

class myThread( threading.Thread ):
    def __init__(self,url,word,title):
        threading.Thread.__init__(self)
        self.url = url  #string for url
        self.id = threading.activeCount()
        self.word = word
        self.title= title

    def run(self):
        # print("Starting thread {id}: {u}".format(id=self.id,u=self.url))
        tree = getSite(self.url)
        wordList = findWord(tree,self.word)
        email = 'To:foukster@gmail.com\nSubject:[NewsParser]BlockchainAlert\n\n'
        if wordList:
			try:
				print(self.title + ": " + self.url)
				email = email + self.title + ": " + self.url + "\n"
				for eachword in wordList:
					print("     "+eachword)
					email = email + "       "+eachword+"\n\n"
				print("\n")
			except:
				email = email + self.url+"\n"
			call("printf '"+email+"' | ssmtp -t",shell=True)


def getSite( url ):
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
            }
    page = requests.get(url,headers=headers)
    tree = html.fromstring(page.text )
    return tree

def findWord( tree, word ):
    wordList = tree.xpath('//p[contains(text(),"{w}")]/text()'.format(w=word))
    return wordList

def grabLinksFromFinviz():
    tree = getSite('https://www.finviz.com/news.ashx')
    linkList = tree.xpath('//div[@id="news"]//tr[@class="nn"]//a[@class="nn-tab-link"]')
    return linkList

def searchNews(word):
    linkList = grabLinksFromFinviz()
    #start new threads for each link
    lasturl = ''
    with open ("/home/pi/lasturl.txt","r") as readfile:
        lasturl = readfile.read()

    with open ("/home/pi/lasturl.txt","w") as writefile:
        writefile.write(linkList[0].attrib.get('href'))

    for link in linkList:
        if link.attrib.get('href') == lasturl:  # don't process what we've already processed
            return
        thread = myThread(link.attrib.get('href'),word,link.text)
        thread.start()
    print "Exiting main thread"


if __name__ == "__main__":
   searchNews('blockchain')