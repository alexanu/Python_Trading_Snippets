from ftplib import FTP

def login():
    ftp = FTP('tickdata.darwinex.com')
    ftp.login('RobotWealth', '1690AqdFORWuo7')
    
    return ftp
    
def transfer_year(ftp, pair, year):

    files = ftp.nlst()
    files = [file for file in files if (file.find(year) > 0)]

    for file in files:
        print(f'Doing {file}...')
        with open(file, 'wb') as f:
            ftp.retrbinary(f'RETR {file}', f.write)
            f.close()
            

if __name__ == "__main__":
    ftp = login()
    pairs = ["EURUSD", "EURNZD", "EURAUD" ] 
    for pair in pairs:
        ftp.cwd(pair)
        transfer_year(ftp, pair, "2020")
        ftp.cwd("..")
    
    ftp.quit()
    