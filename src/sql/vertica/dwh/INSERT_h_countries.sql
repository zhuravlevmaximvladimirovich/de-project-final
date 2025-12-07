INSERT INTO
    VT251201B6F661__DWH.h_countries(hk_country_id, country_name, load_dt, load_src)
SELECT
    HASH(country) AS hk_country_id,
    country AS country_name,
    '{{ ds }}' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.transactions_source
WHERE
    transaction_dt::DATE = '{{ ds }}' AND (HASH(country) NOT IN (SELECT hk_country_id FROM VT251201B6F661__DWH.h_countries))
LIMIT
    1 OVER (PARTITION BY country ORDER BY id DESC);