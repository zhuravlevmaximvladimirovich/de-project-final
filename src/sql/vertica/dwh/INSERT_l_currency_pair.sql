INSERT INTO
    VT251201B6F661__DWH.l_currency_pair(hk_l_currency_pair, hk_currency_id, hk_currency_with_id, load_dt, load_src)
SELECT
    HASH(dc.hk_currency_id, dcw.hk_currency_id) AS hk_l_currency_pair,
    dc.hk_currency_id AS hk_currency_id,
    dcw.hk_currency_id AS hk_currency_with_id,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.currencies_source sc
LEFT JOIN
    VT251201B6F661__DWH.h_currencies dc ON dc.currency_code = sc.currency_code
LEFT JOIN
    VT251201B6F661__DWH.h_currencies dcw ON dcw.currency_code = sc.currency_code_with
WHERE
    sc.date_update = '2022-10-01'
    AND (HASH(dc.hk_currency_id, dcw.hk_currency_id) NOT IN (SELECT hk_l_currency_pair FROM VT251201B6F661__DWH.l_currency_pair))
LIMIT
    1 OVER (PARTITION BY dc.hk_currency_id, dcw.hk_currency_id ORDER BY sc.id DESC);