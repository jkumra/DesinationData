select * from factual_duplicates a where  a.match_percent=70 and a.factual_id in (
select a.factual_id from factual_duplicates a join factual_duplicates2 b on a.factual_id=b.factual_id and a.original_factual_id=b.original_factual_id
where a.match_percent=70
union
select a.factual_id from factual_duplicates a join factual_duplicates2 b on a.factual_id=b.original_factual_id and a.original_factual_id=b.factual_id
where a.match_percent=70)