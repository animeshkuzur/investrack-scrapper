import requests
import MySQLdb
from unidecode import unidecode
from tqdm import tqdm
from datetime import datetime


def scrap(url,days):
	req_date = get_date(days)
	req_url = url+req_date
	print "\n"
	print "Scrapping for the date :"+req_date
	print "Request URL: "+req_url
	try:
		response_code = requests.get(req_url)
		if response_code.status_code == 200:
			content = unidecode(response_code.text) #converting unicode characters to ASCII
			lines = content.split("\n")
			numlines = len(lines)
			for abc in tqdm(range(numlines)): #tqdm() function for progessbar
				data = lines[abc-1].split(";")
				try:
					if data[1]:
						if data[0] > 0:
							sql_insert(data[0],data[1],data[2],data[3],data[4],req_date)	
				except:
					pass
	except requests.ConnectionError:
		print "no internet connection"

	
	return 0

def crawl(url,start,end):
	sdate = start.split("-")
	edate = end.split("-")
	total_days = days_diff(int(edate[0]),int(edate[1]),int(edate[2]))-days_diff(int(sdate[0]),int(sdate[1]),int(sdate[2]))
	print "Total number of days: "+str(total_days)
	ending_days = days_diff(int(edate[0]),int(edate[1]),int(edate[2]))
	starting_days = days_diff(int(sdate[0]),int(sdate[1]),int(sdate[2]))
	for i in range(starting_days,ending_days):
		scrap(url,i)
	return 0

def get_date(g):
	#Algorithm to calculate the date from number of days
	y = (10000*g + 14780)/3652425
	ddd = g - (365*y + y/4 - y/100 + y/400)
	if (ddd < 0):
		y = y - 1
		ddd = g - (365*y + y/4 - y/100 + y/400)
	mi = (100*ddd + 52)/3060
	mm = (mi + 2)%12 + 1
	y = y + (mi + 2)/12
	dd = ddd - (mi*306 + 5)/10 + 1
	date = str(y)+"-"+str(mm)+"-"+str(dd)
	return date

def days_diff(y,m,d):
	#Algorithm to calculate the number of days
	m = (m + 9) % 12
	y = y - m/10
	days = 365*y + y/4 - y/100 + y/400 + (m*306 + 5)/10 + ( d - 1 )
	return days

def sql_insert(scode,sname,asset,repurchase,sale,date):
	try:
	 	payout = "-"
	 	reinvest = "-"
	 	conn = MySQLdb.connect("localhost","investrack_root","Investrack@123#","investrack")
	 	c = conn.cursor()
	 	c.execute("""INSERT INTO nav_detail (schemeCode,iSINDivPayout,iSINDivReinvestment,SchemeName,NetAssetValue,RepurchasePrice,SalePrice,policyDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(scode,payout,reinvest,sname,asset,repurchase,sale,date))
	 	conn.commit()
	 	c.close()
	except e:
		#Handling error
	 	print "SQL insertion failure"
	 	print "Logging error."
	 	f = open('error.log','ab') #Name of the error log file
	 	s = "["+str(datetime.now())+"] - |"+str(scode)+"|"+str(sname)+"|"+str(date)+"| : "+str(e)+"\n"
	 	f.write(s);
	 	print e
	 	f.close()
	return 0


url = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=" #URL to crawl

start_date = "2008-01-01" #Enter the initial date
print "Start Date : "+start_date

end_date = "2016-11-15" # Enter the final date
print "End Date : "+end_date

crawl(url,start_date,end_date)