with outcomeCodes (CATEGORY) as
(
	select distinct FIRST_PREG_CATEGORY from @resultsDatabaseSchema.outcome_limit
	where first_preg_category='SA'
),
numberedOutcomeEvents AS
(
	SELECT p.EVENT_ID, p.PERSON_ID, p.Category, p.gest_value, p.EVENT_DATE,
	row_number() over (partition by p.PERSON_ID order by p.event_date, p.event_id) as rn
	FROM #pregnancy_events p
	JOIN #PregnancyEvents e on p.person_id = e.person_id and p.EVENT_ID = e.EVENT_ID
	JOIN outcomeCodes oc on p.CATEGORY = oc.CATEGORY
)
SELECT PERSON_ID, EVENT_ID
INTO @resultsDatabaseSchema.FirstOutcomeEvent
FROM numberedOutcomeEvents
WHERE rn = 1;
