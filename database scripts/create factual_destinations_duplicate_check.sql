

CREATE TABLE factual_duplicates_last_check
(
  last_id_checked integer NOT NULL,
  checked_on timestamp without time zone DEFAULT now()
)
WITH (
  OIDS=FALSE
);
ALTER TABLE factual_duplicates_last_check
  OWNER TO postgres;

insert into factual_duplicates_last_check values (0,now())