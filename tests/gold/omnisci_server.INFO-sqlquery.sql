SELECT min(created_at) AS min_val, max(created_at) AS max_val
FROM github_1k WHERE ((github_1k.org_login = 'Microsoft'));

SELECT github_1k.repo_name AS key0,COUNT(*) AS col0 FROM github_1k WHERE (github_1k.org_login = 'Microsoft') GROUP BY key0 ORDER BY col0 DESC NULLS LAST LIMIT 50 OFFSET 0;

SELECT github_1k.type AS key0,COUNT(*) AS val FROM github_1k WHERE (github_1k.org_login = 'Microsoft') GROUP BY key0 ORDER BY val DESC NULLS LAST LIMIT 500;

select count(*) from tab;

