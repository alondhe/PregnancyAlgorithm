COPY @resultsDatabaseSchema.pregnancy_concepts
FROM 's3://@s3RepoName/@pathToFiles/pregnancy_concepts.csv'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv quote as '`';

COPY @resultsDatabaseSchema.outcome_limit
FROM 's3://@s3RepoName/@pathToFiles/outcome_limit.csv'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv quote as '`';

COPY @resultsDatabaseSchema.term_durations
FROM 's3://@s3RepoName/@pathToFiles/term_durations.csv'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv quote as '`';

COPY @resultsDatabaseSchema.gest_est
FROM 's3://@s3RepoName/@pathToFiles/gest_est.csv'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv quote as '`';
