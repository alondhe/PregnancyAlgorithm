-- Step 5. populate #ValidOutcomes with 'true' ECT pregnancy outcome dates

with cteTargetPeople (person_id) as
(
  select distinct person_id
  from #pregnancy_events pe
  where pe.category = 'ECT'
),
cteTargetCategory (category) as
(
  select first_preg_category as catgory from @resultsDatabaseSchema.outcome_limit
  UNION
  select outcome_preg_category as category from @resultsDatabaseSchema.outcome_limit
  UNION
  select 'AGP' as category
  UNION
  select 'PCONF' as category
  UNION
  select 'ECT_SURG1' as category
  UNION
  select 'ECT_SURG2' as category
  UNION
  select 'MTX' as category
)
SELECT pe.person_id, min(pe.EVENT_ID) as event_id
INTO #PregnancyEvents
from #pregnancy_events pe
join cteTargetPeople tp on pe.person_id = tp.person_id
join cteTargetCategory tc on tc.category = pe.category
group by pe.person_id, pe.category, pe.event_date, pe.gest_value
;
