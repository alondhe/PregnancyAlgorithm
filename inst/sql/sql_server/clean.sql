IF OBJECT_ID('@resultsDatabaseSchema.FirstOutcomeEvent', 'U') IS NOT NULL
DROP TABLE @resultsDatabaseSchema.FirstOutcomeEvent;

IF OBJECT_ID('@resultsDatabaseSchema.pregnancy_concepts', 'U') IS NOT NULL
DROP TABLE @resultsDatabaseSchema.pregnancy_concepts;

IF OBJECT_ID('@resultsDatabaseSchema.outcome_limit', 'U') IS NOT NULL
DROP TABLE @resultsDatabaseSchema.outcome_limit;

IF OBJECT_ID('@resultsDatabaseSchema.gest_est', 'U') IS NOT NULL
DROP TABLE @resultsDatabaseSchema.gest_est;

IF OBJECT_ID('@resultsDatabaseSchema.term_durations', 'U') IS NOT NULL
DROP TABLE @resultsDatabaseSchema.term_durations;


