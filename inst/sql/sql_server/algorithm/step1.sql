select a.category, a.gest_value, a.person_id, a.start_date, a.type, a.value_as_string, a.value_as_number
into #raw_events
from
(
	select b.category, b.gest_value, person_id, a.CONDITION_CONCEPT_ID as CONCEPT_ID,  condition_start_date as start_date,  'Cond' as type, ' ' as value_as_string,null as value_as_number
	from @cdmDatabaseSchema.condition_occurrence a
	join @resultsDatabaseSchema.pregnancy_concepts b on a.CONDITION_CONCEPT_ID=b.concept_id

	union all
	select b.category, b.gest_value, person_id, a.PROCEDURE_CONCEPT_ID as CONCEPT_ID, procedure_date as start_date, 'Proc' as type, ' ' as value_as_string,null as value_as_number
	from @cdmDatabaseSchema.procedure_occurrence a
	join @resultsDatabaseSchema.pregnancy_concepts b on a.PROCEDURE_CONCEPT_ID=b.concept_id

	union all
	select b.category, b.gest_value, person_id, a.OBSERVATION_CONCEPT_ID as CONCEPT_ID, observation_date as start_date, 'Obs' as type, value_as_string, value_as_number
	from @cdmDatabaseSchema.observation a
	join @resultsDatabaseSchema.pregnancy_concepts b on a.OBSERVATION_CONCEPT_ID=b.concept_id
	where b.data_value = 'N/A' or (b.data_value = a.value_as_string)

	union all
	select b.category, b.gest_value, person_id, a.MEASUREMENT_CONCEPT_ID as CONCEPT_ID, measurement_date as start_date, 'Meas' as type, value_source_value, value_as_number
	from @cdmDatabaseSchema.measurement a
	join @resultsDatabaseSchema.pregnancy_concepts b on a.measurement_CONCEPT_ID=b.concept_id
	where b.data_value = 'N/A' or (b.data_value = a.value_source_value)
) a
;

with cteDiabetesMembers as /* Patients with a Glucose test record */
(
	select distinct person_id from #raw_events where category = 'DIAB'
)
select PERSON_ID, CATEGORY, GEST_VALUE, START_DATE
into #events_nondrug
from
(
  select PERSON_ID,
  Category,
  /*convert gestational weeks to days */
  case when category='GEST' and value_as_number is not null then 7*value_as_number
      when category='GEST' and gest_value is not null then 7*gest_value
  	  else null end as gest_value,
  start_date
  from #raw_events
  where person_id not in /* exclude people who have ONLY glucose tests */
  (
  	select e.person_id
  	from #raw_events e
  	join cteDiabetesMembers m on e.person_id = m.person_id
  	group by e.person_id
  	having count(distinct e.category) = 1
  )
) a
where not (category='GEST' and (gest_value is null or gest_value>301))
;

select f.person_id, f.category, f.start_date, f.gest_value
INTO #events_drug
from (
  select distinct person_id from #events_nondrug
) a
join (
  select person_id, a.DRUG_CONCEPT_ID as CONCEPT_ID,  DRUG_exposure_start_date as start_date,
    'Drug' as type, ' ' as value_as_string, null as value_as_number, e.gest_value, e.category
	from @cdmDatabaseSchema.drug_exposure a
	join (
	  select c.concept_id, c.concept_name, d.gest_value, d.category
    from @cdmDatabaseSchema.concept c
    join (
      select b.descendant_concept_id, a.concept_id, a.category, a.gest_value
      from @cdmDatabaseSchema.concept_ancestor b
      join (
        select concept_id, category, gest_value
        from @resultsDatabaseSchema.pregnancy_concepts
        where category in ('CONTRA','MTX','OVULDR')
      ) a on a.concept_id=b.ANCESTOR_CONCEPT_ID
    ) d on c.concept_id=d.DESCENDANT_CONCEPT_ID
    where c.concept_class_id in ('Branded Drug','Branded Pack','Clinical Drug','Clinical Pack','Ingredient')
  ) e on a.DRUG_CONCEPT_ID=e.concept_id
) f on a.person_id=f.person_id
;

select person_id, category, event_date, gest_value
INTO #events_all
FROM  (
	select person_id, category, start_date as event_date, gest_value
	from #events_nondrug

	UNION ALL
	select person_id, category, start_date as event_date, gest_value
	from #events_drug
) E
;

select PERSON_ID, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date) AS event_id,
  category, event_date, gest_value
into #pregnancy_events
from #events_all
;

TRUNCATE TABLE #events_drug;
DROP TABLE #events_drug;

TRUNCATE TABLE #events_nondrug;
DROP TABLE #events_nondrug;

TRUNCATE TABLE #events_all;
DROP TABLE #events_all;

TRUNCATE TABLE  #raw_events;
DROP TABLE  #raw_events;

-- Step 1. Initlaize temp tables

CREATE TABLE #ValidOutcomes
(
  PERSON_ID bigint not null,
	EVENT_ID int not null
)
;
