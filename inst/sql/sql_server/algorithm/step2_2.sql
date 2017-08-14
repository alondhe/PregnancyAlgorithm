INSERT INTO #ValidOutcomes (PERSON_ID, EVENT_ID)
select PERSON_ID, EVENT_ID from @resultsDatabaseSchema.FirstOutcomeEvent;

select e.PERSON_ID, e.EVENT_ID
INTO #deletedEvents
FROM #PregnancyEvents e
JOIN #pregnancy_events pe on e.PERSON_ID = pe.PERSON_ID and pe.EVENT_ID = e.EVENT_ID
JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo on fo.PERSON_ID = pe.PERSON_ID
JOIN #pregnancy_events foe on foe.person_id = fo.person_id and foe.EVENT_ID = fo.EVENT_ID
JOIN @resultsDatabaseSchema.outcome_limit ol on ol.FIRST_PREG_CATEGORY = foe.Category AND ol.OUTCOME_PREG_CATEGORY = pe.Category
WHERE (datediff(d,foe.EVENT_DATE, pe.EVENT_DATE) + 1) < ol.MIN_DAYS
;


with cteTargetPeople (person_id) as
(
  select distinct e.person_id
  from #PregnancyEvents e
  join #pregnancy_events pe on e.person_id = pe.person_id and e.event_id = pe.event_id
  where pe.category = 'LB'
)
select pe.PERSON_ID, pe.EVENT_ID
INTO #temp_PregnancyEvents
FROM #PregnancyEvents pe
join cteTargetPeople p on pe.person_id = p.person_id
left join #deletedEvents de on pe.person_id = de.person_id and pe.event_id = de.event_id
where de.person_id is null;

TRUNCATE TABLE #deletedEvents;
DROP TABLE #deletedEvents;

TRUNCATE TABLE #PregnancyEvents;
DROP TABLE #PregnancyEvents;

SELECT PERSON_ID, EVENT_ID
INTO #PregnancyEvents
from #temp_PregnancyEvents
;

TRUNCATE TABLE #temp_PregnancyEvents;
DROP TABLE #temp_PregnancyEvents;

TRUNCATE TABLE @resultsDatabaseSchema.FirstOutcomeEvent;
DROP TABLE @resultsDatabaseSchema.FirstOutcomeEvent;
