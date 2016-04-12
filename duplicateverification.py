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

class factual_pair:
	def __init__(self,original_id,duplicate_id):
		self.original_id = original_id
		self.duplicate_id=duplicate_id
statsd = StatsClient()
start = time.time()

conn= dbapi2.connect(database="socialtopias-new",user="postgres",password="postgres" )
cur=conn.cursor()

sourcelist=dict()
comparelist1=dict()
comparelist2=dict()
sourcegroup=dict()
comparegroup1=dict()
comparegroup2=dict()
results=dict()
source=dict()
compare1=dict()
compare2=dict()



def populatelists(cur,query,listtopopulate,grouptopopulate):
	cur.execute(query)
	rows=cur.fetchall()
	group=0
	rowcount=0
	for row in rows:
		try:
			group+=1
			factual_ids=row[0]
			original_id=row[1]
			id_list=list()
			listtopopulate[original_id]=group
			id_list.append(original_id)
			
			#print(group,original_id,factual_ids)
			fidlist=factual_ids.split(";")
			count=len(fidlist)
			
			
			for i in range(count):
				duplicate_id=fidlist[i]
				listtopopulate[duplicate_id]=group
				id_list.append(duplicate_id)
				
			grouptopopulate[group]=id_list
				
		except:
				print(sys.exc_info()[0])
				raise


source="python fuzzywuzzy 70"
compare1="python fuzzywuzzy 60"
compare2=".Net DuoVia.FuzzyStrings"
query="""select string_agg(a.duplicate_factual_Id::text,';') as factual_ids, original_factual_id,duplicate_check_type   from factual_duplicates a where duplicate_check_type=1
group by duplicate_check_type,original_factual_id """
populatelists(cur,query,sourcelist,sourcegroup)
duplicate_source_type=1

query="""select string_agg(a.duplicate_factual_Id::text,';') as factual_ids, original_factual_id,duplicate_check_type  from factual_duplicates a where duplicate_check_type=2
group by duplicate_check_type,original_factual_id """
populatelists(cur,query,comparelist1,comparegroup1)	
verification_source1_type=2


query="""select string_agg(a.duplicate_factual_Id::text,';') as factual_ids, original_factual_id,duplicate_check_type  from factual_duplicates a where duplicate_check_type=3
group by duplicate_check_type, original_factual_id """
populatelists(cur,query,comparelist2,comparegroup2)	
verification_source2_type=3		

query="SELECT COALESCE(MAX(duplicate_check_batch_number),0) AS batch_number FROM factual_duplicates"
cur.execute(query)
row=cur.fetchone()
batch_number=row[0]+1;

rowcount=0
for id_to_verify in sourcelist:
	rowcount+=1
	group_number=sourcelist[id_to_verify]
	id_list=sourcegroup[group_number]
	source_original_id=id_list[0]
	foundingroup1=False
	originalid1=""
	foundingroup2=False
	originalid2=""
	if id_to_verify!=source_original_id:
		if id_to_verify in comparelist1:
			group_number=comparelist1[id_to_verify]
			id_list=comparegroup1[group_number]
			originalid1=id_list[0]
			if source_original_id in id_list:
				foundingroup1=True
		if id_to_verify in comparelist2:
			group_number=comparelist2[id_to_verify]
			id_list=comparegroup2[group_number]
			originalid2=id_list[0]
			if source_original_id in id_list:
				foundingroup2=True
		results[id_to_verify]=(source_original_id,foundingroup1,originalid1,foundingroup2,originalid2)
		print(rowcount,id_to_verify,results[id_to_verify])	
		query="""INSERT INTO factual_duplicates_verification(
             duplicate_source_type, verified_duplicate_factual_id, original_factual_id, 
            verification_source1_type,found_source1, source1_original_factual_id, 
			verification_source2_type,found_source2, source2_original_factual_id, 
            verified_on, verification_check_batch_number) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)"""
		#parameters=list()
		source_type="1"
		parameters=(duplicate_source_type,id_to_verify,source_original_id,
		verification_source1_type, foundingroup1,originalid1,
		verification_source2_type,foundingroup2,originalid2,batch_number)
		cur.execute(query,parameters)
conn.commit()

input("Press Enter to continue...")
	
	
cur.close()
conn.close()
