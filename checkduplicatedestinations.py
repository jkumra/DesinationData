import string
import re
import sys,getopt
import psycopg2 as dbapi2
import time
import chardet
from statsd import StatsClient
import os
import logging
from logging.config import fileConfig
import logging.handlers
import json

def setup_logging(
    default_path='my_logging.json', 
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def normalize(s):
	for p in string.punctuation:
		s = s.replace(p, ' ')
	noisewords=["the", "and", "&", "bar", "grill", "restaurant", "cafe", "hotel", "airport", ]
	for p in noisewords:
		s = s.lower().replace(p, ' ')
	return s.strip()
def stringcompare(s1,s2):
	print (fuzz.ratio(s1,s2))
	print (fuzz.partial_ratio(s1,s2))
	fuzz.token_sort_ratio(s1,s2)
	choices = ["Atlanta Falcons", "New York Jets", "New York Giants", "Dallas Cowboys"]
	process.extract("new york jets", choices, limit=2)
    #[('New York Jets', 100), ('New York Giants', 78)]
	process.extractOne("cowboys", choices)
    #("Dallas Cowboys", 90)
def addresscompare(a1,a2):
	a1numbers=re.f(r'\d+', a1)
	a2numbers=re.findall(r'\d+', a2)

	if (len(a1numbers) > 0 and len(a2numbers) > 0):
		if (a1numbers[0]!=a2numbers[0]):
			return False
	if (fuzz.token_sort_ratio(a1,a2) > match_percent):
		return True
	else:
		return False

def namecompare(n1,n2):
	n1=normalize(n1)
	n2=normalize(n2)
	if (fuzz.token_sort_ratio(n1,n2) > match_percent):
		#print(fuzz.token_sort_ratio(n1,n2), n1,":",n2)
		return True
	else:
		return False

		
setup_logging()

logger = logging.getLogger("info_file_handler")
optlist,arglist= getopt.getopt(sys.argv[1:],"p:c:s:h")
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
duplicate_check_type=2
match_percent=65
match_library="fuzzy logic"
cityid= 14608 # 14869
#print(comments)
#sys.exit()
cityname=""
statename=""
for opt,arg in optlist:

	if opt=="-h":
		print("cleancitydata.py -c <cityname> -s <statename>")
		sys.exit()
	elif opt=="-c":
		#cityname=arg
		cityid=arg
	elif opt=="-s":
		statename=arg
		print("statename:",statename)
	elif opt=="-p":
		sort_ratio=arg
		sort_ratio_value=int(sort_ratio)
		
#comments=str(sort_ratio_value)+"% "+match_library+ " match"
#cityname="charlotte"
#statename="nc"
#cityid=14715
#if cityname=="" or statename=="":
#	sys.exit()
#query="""UPDATE FACTUAL_DESTINATIONS SET DELETED=TRUE WHERE UPPER(LOCALITY)='CHARLOTTE'"""
#query="""UPDATE FACTUAL_DESTINATIONS SET DELETED=FALSE WHERE UPPER(LOCALITY)='CHARLOTTE' AND trim(replace(replace(CATEGORY_IDS,'[',''),']','')) IN (SELECT CATEGORY_ID::TEXT FROM FACTUAL_CATEGORIES WHERE KEEP_FLG=TRUE)"""
#query="""SELECT ID,FACTUAL_ID, NAME, ADDRESS,LOCALITY,REGION,POSTCODE FROM FACTUAL_DESTINATIONS WHERE DELETED=FALSE AND UPPER(REGION)=%s
#AND UPPER(LOCALITY)=%s ORDER BY ADDRESS"""

id_to_keep=list();
duplicate_id=list();
previousaddress=""
newaddress=""
rowcount=0
originals=list()
duplicates=dict()
duplicateid_list=list()
duplicatecount=0
exceptioncount=0
exceptionlist=list()

statsd = StatsClient()
start = time.time()

conn= dbapi2.connect(database="socialtopias-new",user="postgres",password="postgres" )
cur=conn.cursor()

query="SELECT COALESCE(MAX(duplicate_check_batch_number),0) AS batch_number FROM factual_duplicates"
cur.execute(query)
row=cur.fetchone()
batch_number=row[0]+1;


query ="""SELECT  COALESCE(max(last_city_checked),0) from factual_duplicates_last_check"""
cur.execute(query)
row=cur.fetchone()
last_city_checked=row[0]+1

query="""select distinct region  from factual_cities where id > %s order by region"""
parameters=list()
parameters.append(last_city_checked)
cur.execute(query,parameters)
#query ="""select id, region,locality from factual_cities where id =%s"""
#parameters=list()
#parameters.append(cityid)
#cur.execute(query,parameters)

locations=cur.fetchall()
factual_items=dict()
for location in locations:
	
	statename=location[0]
	#cityname=location[2]
	print("Begining:",statename)
	logger.info("Begining State:%s ",statename)
	
	query="""SELECT c.id, MAX(a.Id) max_id,  string_agg(a.Id::text,';' order by FACTUAL_ID) as Ids, string_agg(FACTUAL_ID,';' order by FACTUAL_ID) as FactualIds,  string_agg(a.NAME,'";"' order by FACTUAL_ID) as Names,
	TRIM(ADDRESS),a.LOCALITY,a.REGION FROM FACTUAL_DESTINATIONS a Join factual_Cities c on a.locality=c.locality and a.region=c.region
	WHERE UPPER(a.REGION)=%s and c.id >%s
	AND  TRIM(ADDRESS)<>'' Group By c.id, a.locality, a.REGION,TRIM(a.ADDRESS) having count(*) > 1
	UNION
	SELECT c.id, MAX(a.Id) max_id,  string_agg(a.Id::text,';' order by FACTUAL_ID) as Ids, string_agg(FACTUAL_ID,';' order by FACTUAL_ID) as FactualIds,  string_agg(a.NAME,'";"' order by FACTUAL_ID) as Names,
	'' as Address,a.LOCALITY,a.REGION FROM FACTUAL_DESTINATIONS a Join factual_Cities c on a.locality=c.locality and a.region=c.region
	WHERE UPPER(a.REGION)=%s and c.id > %s
	AND  TRIM(ADDRESS)='' Group By c.id, a.locality, a.REGION,LATITUDE,LONGITUDE having count(*) > 1"""
	parameters=list()
	parameters.append(statename.upper())
	parameters.append(last_city_checked)
	parameters.append(statename.upper())
	parameters.append(last_city_checked)
	#parameters=(statename.upper(),last_city_checked,statename.upper(),cityname.upper())
	cur.execute(query,parameters)
	rows=cur.fetchall()
	#query1="INSERT INTO factual_duplicates (factual_id,original_factual_id,duplicate_check_batch_number,comments,match_percent) VALUES (%s,%s,%s,%s,%s)"
	query1="""INSERT INTO factual_duplicates (duplicate_check_type, duplicate_factual_id, duplicate_name, 
            duplicate_address, original_factual_id, original_name, original_address, 
            match_percent, duplicate_check_batch_number, inserted_on, comments)
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)"""
			
	for row in rows:
		try:
			rowcount+=1
			print (rowcount)
			ids=row[2]
			names=row[4]
			factual_ids=row[3]
			address=row[5] +"," +row[6] +"," +row[7]
			namelist=names.split("\";\"")
			fidlist=factual_ids.split(";")
			namecount=len(namelist)
			maxid=row[1]
			last_city_checked=row[0]
			duplicatefound=False
			logger.info("Starting MaxID:%s",maxid)
			duplicateid_list.clear()
			duplicates.clear()
			factual_items.clear()
			for i in range(namecount):
				name1=namelist[i]
				id1=fidlist[i]
				for j in range(i+1,namecount):
					name2=namelist[j]
					id2=fidlist[j]
					result = namecompare(name1,name2)
					#if id1==1177810 or id2==1177810:
					try:
						print(result , id2,":",name2.encode("utf-8"),"   ", id1, ":",name1.encode("utf-8"))
					except:
						logger.exception("Exception in printing:")
					if result==True:
						duplicatefound=True
						if id2 not in duplicateid_list:
							duplicateid_list.append(id2)
							if id1 not in factual_items:
								factual_items[id1]=(name1,address)
							if id2 not in factual_items:
								factual_items[id2]=(name2,address)
							duplicatecount+=1
							if id1 in duplicateid_list:
								id1=duplicates[id1]
															
							duplicates[id2]=id1
							#print(id2,":",name2.encode("utf-8")," duplicate of  ", id1, ":",name1.encode("utf-8"))
			if duplicatefound ==True:
				for d in duplicates:
					name1=factual_items[d][0]
					address1=factual_items[d][1]
					id=duplicates[d]
					name2=factual_items[id][0]
					address2=factual_items[id][1]
					parameters=(duplicate_check_type,d,name1,address1,id,name2,address2,match_percent,batch_number,match_library)
					#print(query1,parameters)
					print(d,id)
					cur.execute(query1,parameters)
				#conn.commit()
				
			
		except:
				exceptioncount+=1
				exceptionlist.append(sys.exc_info()[0])
				logger.exception("Exception:")
				
	query2="update factual_duplicates_last_check set checked_on=now(),last_city_checked=%s"
	values=list()
	values.append(last_city_checked)
	cur.execute(query2,values)
	conn.commit()
	print("Completed:",cityname,statename)
	logger.info("Completed:%s %s",cityname,statename)
	
print ("total rows:",rowcount)
print ("duplicates:",duplicatecount)
print(exceptionlist)
print("total Exceptions:",exceptioncount)
dt = int((time.time() - start) )
statsd.timing('slept', dt)
print("total time taken (s):",dt)
logger.info("Total Rows:%s",rowcount)
logger.info("Duplicate Rows:%s",duplicatecount)
logger.info("Total Exceptions:%s",exceptioncount)
logger.info("Total time taken (s):%s",dt)

cur.close()
conn.close()
