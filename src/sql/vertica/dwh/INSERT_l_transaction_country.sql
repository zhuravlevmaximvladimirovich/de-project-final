INSERT INTO
    VT251201B6F661__DWH.l_transaction_country(hk_l_transaction_country, hk_transaction_id, hk_country_id, load_dt, load_src)
SELECT
    HASH(dt.hk_transaction_id, dc.hk_country_id) AS hk_l_transaction_country,
    dt.hk_transaction_id AS hk_transaction_id,
    dc.hk_country_id AS hk_country_id,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.transactions_source st
LEFT JOIN
    VT251201B6F661__DWH.h_transactions dt ON dt.operation_id = st.operation_id
LEFT JOIN
    VT251201B6F661__DWH.h_countries dc ON dc.country_name = st.country
WHERE
    st.transaction_dt::DATE = '2022-10-01'
    AND (HASH(dt.hk_transaction_id, dc.hk_country_id) NOT IN (SELECT hk_l_transaction_country FROM VT251201B6F661__DWH.l_transaction_country))
LIMIT
    1 OVER (PARTITION BY hk_transaction_id, hk_country_id ORDER BY st.id DESC);