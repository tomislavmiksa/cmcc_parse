import requests
from db_stuff import DBmarket
year="2023"

def monthReplace(input):
    input = input.replace("January ", "{0}-02-".format(year) )
    input = input.replace("February ", "{0}-02-".format(year) )
    input = input.replace("March ", "{0}-02-".format(year) )
    input = input.replace("April ", "{0}-02-".format(year) )
    input = input.replace("May ", "{0}-02-".format(year) )
    input = input.replace("June ", "{0}-02-".format(year) )
    input = input.replace("July ", "{0}-02-".format(year) )
    input = input.replace("August ", "{0}-02-".format(year) )
    input = input.replace("September ", "{0}-02-".format(year) )
    input = input.replace("October ", "{0}-02-".format(year) )
    input = input.replace("November ", "{0}-02-".format(year) )
    input = input.replace("December ", "{0}-02-".format(year) )
    return input

def parseReport(url):
    result = []
    r = requests.get(url)
    p, symbol, psymbol = "", "", ""
    for l in r.text.split('\n'):
        record = {}
        if "Disaggregated Commitments of Traders" in l:
            list = p.split()
            code = list[-1]
            list.pop()
            symbol = " ".join(list)
            d   = ( l.split(',') )[1].strip()
            y   = ( l.split(',') )[2].strip()
            date = monthReplace( "{0} {1}".format(y, d) )
        if "All  :" in l:
            important = l.split()
            long  = important[8].replace(",","")
            short = important[9].replace(",","")
            if (symbol != psymbol) and (long not in ["0", "0.1"] and short not in ["0", "0.1"]):
                ye = ( date.split('-') )[0]
                ye = ( ye.split() )[0]
                record = {
                    "year"  : ye,
                    "month" : ( date.split('-') )[1],
                    "day" : ( date.split('-') )[2],
                    "code"  : code,
                    "longPositions"  : long,
                    "shortPositions" : short,
                    "symbol" : symbol
                }
                result.append( record )
                psymbol = symbol
        p = l
    return result

def parseFinancial(url):
    result = []
    r = requests.get(url)
    p, symbol, psymbol = "", "", ""
    for l in r.text.split('\n'):
        if l.startswith("CFTC Code"):
            code = ( l.split() )[1] + '-' + ( l.split() )[2]
            symbol = " ".join( p.split() )
        if "Traders in Financial Futures" in l:
            y = ( l.split() )[-1]
            d = ( l.split() )[-2].replace(",", "")
            m = ( l.split() )[-3]
            date = monthReplace( "{0} {1}".format(m, d) )
        if p.startswith("Positions"):
            important = l.split()
            long  = important[3].replace(",","")
            short = important[4].replace(",","")
            if (long not in ["0", "0.1"] and short not in ["0", "0.1"]):
                record = {
                    "year"  : ( date.split('-') )[0],
                    "month" : ( date.split('-') )[1],
                    "day" : ( date.split('-') )[2],
                    "code"  : code,
                    "longPositions"  : long,
                    "shortPositions" : short,
                    "symbol" : symbol
                }
                result.append( record )
                psymbol = symbol
        p = l
    return result

if __name__ == "__main__":
    market = DBmarket(r".\db\pythonsqlite.db")

    stdReport = open('stdReports.txt', 'r')
    line = stdReport.readline().strip()
    while line:
        print(line)
        r = parseReport(line)
        market.add_records(r)
        line = stdReport.readline().strip()
    stdReport.close()

    finReport = open('financialReports.txt', 'r')
    line = finReport.readline().strip()
    while line:
        print(line)
        r = parseFinancial(line)
        market.add_records(r)
        line = finReport.readline().strip()
    finReport.close()
