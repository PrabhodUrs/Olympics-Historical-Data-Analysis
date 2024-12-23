/* count of each type of medal for each country */
SELECT
    o.country_name, r.medal,
    COUNT(DISTINCT CASE 
            WHEN e.event_type = 'team' THEN
                CONCAT(r.year, '_', e.event_id)
            WHEN e.event_type = 'individual' THEN 
                CONCAT(o.olympian_id,'_',r.year,'_',e.event_id)
            ELSE NULL 
        END) AS total_medals
FROM
    olympians o
INNER JOIN
    results r
ON
    o.olympian_id = r.olympian_id
INNER JOIN
    events e
ON
    r.event_id = e.event_id
GROUP BY
    o.country_name, r.medal
ORDER BY
    o.country_name, r.medal


/* total medals by each country */
SELECT
    o.country_name,
    COUNT(DISTINCT CASE 
            WHEN e.event_type = 'team' THEN
                CONCAT(r.year, '_', e.event_id)
            WHEN e.event_type = 'individual' THEN 
                CONCAT(o.olympian_id,'_',r.year,'_',e.event_id)
            ELSE NULL 
        END) AS total_medals
FROM
    olympians o
INNER JOIN
    results r
ON
    o.olympian_id = r.olympian_id
INNER JOIN
    events e
ON
    r.event_id = e.event_id
WHERE
    r.medal != 'No medal'
GROUP BY
    o.country_name
ORDER BY
    total_medals DESC