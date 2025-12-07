INSERT INTO 
    VT251201B6F661__DWH.s_currency_pair(hk_l_currency_pair, currency_code_div, date_update, load_dt, load_src)
SELECT
    dcp.hk_l_currency_pair AS hk_l_currency_pair,
    sc.currency_code_div AS currency_code_div,
    sc.date_update AS date_update,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.currencies_source sc
LEFT JOIN
    VT251201B6F661__DWH.h_currencies dc ON dc.currency_code = sc.currency_code
LEFT JOIN
    VT251201B6F661__DWH.h_currencies dcw ON dcw.currency_code = sc.currency_code_with
LEFT JOIN
    VT251201B6F661__DWH.l_currency_pair dcp ON dcp.hk_currency_id = dc.hk_currency_id AND dcp.hk_currency_with_id = dcw.hk_currency_id
WHERE
    sc.date_update = '2022-10-01'
LIMIT
    1 OVER (PARTITION BY hk_l_currency_pair, date_update, currency_code_div ORDER BY sc.id DESC);