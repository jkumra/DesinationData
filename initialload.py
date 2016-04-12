import sys,getopt
import csv
import psycopg2 as dbapi2
import time
from statsd import StatsClient
optlist,arglist= getopt.getopt(sys.argv[1:],"f:h")
filename=""
city="Charlotte"
state="nc"
city=city.upper()
state=state.upper()
rowval=list()
uncommittedrowcount=0
for opt,arg in optlist:
	if opt=="-h":
		print("loadcitydata.py -f <filename>")
		sys.exit()
	elif opt=="-f":
		filename=arg
print("file:",filename)
if filename=="":
	sys.exit()
validcount=0
rowcount=0
exceptioncount=0
exceptionlist=list()

statsd = StatsClient()
start = time.time()

conn= dbapi2.connect(database="socialtopias-new",user="postgres",password="postgres" )
cur=conn.cursor()
query="""select category_id from factual_category_inclusion_list order by category_id"""
cur.execute(query)
rows=cur.fetchall()
category_ids_included=list()
for row in rows:
	category_ids_included.append('%i' % row[0])
with open(filename, encoding="utf8") as csvfile:
	query="""INSERT INTO factual_destinations(factual_id, name, address, address_extended, po_box, locality, 
region, post_town, admin_region, postcode, country, tel, fax, 
latitude, longitude, neighborhood, website, email, category_ids, 
category_labels, chain_name, chain_id, hours, hours_display, 
existence) Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
	for line in csv.reader(csvfile, delimiter="\t"): # dialect="excel-tab"You can also use delimiter="\t" rather than giving a dialect.
		try:
			rowcount+=1
			category_id=line[18].replace("[","").replace("]","").replace(" ","")
			category_ids=category_id.split(",")
			category_label=line[19]
			common=list(set(category_ids) & set(category_ids_included))
			#print (category_ids,common)
			existence_score=line[24]
			if rowcount>1 and float(existence_score)>=.3 and len(common) > 0:
				validcount+=1
				print(rowcount)
				cur.execute(query,line)
				uncommittedrowcount+=1
			if uncommittedrowcount>=10000:
				#print("commiting")
				#break
				conn.commit()
				uncommittedrowcount=0
		except:
			exceptioncount+=1
			exceptionlist.append(sys.exc_info()[0])
			#raise
if uncommittedrowcount>0:
	conn.commit()
cur.close()
conn.close()
dt = int((time.time() - start) )
statsd.timing('slept', dt)

print(exceptionlist)
print("total Exceptions:",exceptioncount)
print("Total Rows vs uploaded Rows:", rowcount,",",validcount)
print("total time taken (s):",dt)