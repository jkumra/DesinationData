Select * from 
( select distinct factual_id, original_factual_id from factual_duplicates where match_percent=70) a join
(

select id||';'||original||';' as groupid from (
select  string_agg(FACTUAL_ID,';') as id,original_factual_id original
from factual_duplicates2 by group by original_factual_id) g)
 b on POSITION(a.factual_id IN b.groupid)<>0 and POSITION(a.original_factual_id IN b.groupid)<>0