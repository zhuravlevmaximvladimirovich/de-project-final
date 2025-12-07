INSERT INTO
    VT251201B6F661__DWH.h_currencies(hk_currency_id, currency_code, load_dt, load_src)
SELECT
    HASH(currency_code) AS hk_currency_id,
    currency_code,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.currencies_source
WHERE
    date_update = '2022-10-01' AND (HASH(currency_code) NOT IN (SELECT hk_currency_id FROM VT251201B6F661__DWH.h_currencies))
LIMIT
    1 OVER (PARTITION BY currency_code ORDER BY id DESC);