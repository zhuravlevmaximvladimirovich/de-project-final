INSERT INTO
    VT251201B6F661__DWH.s_transaction_status(hk_transaction_id, status, status_dt, load_dt, load_src)
SELECT
    dt.hk_transaction_id AS hk_transaction_id,
    st.status AS status,
    dt.transaction_dt AS status_dt,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.transactions_source st
LEFT JOIN
    VT251201B6F661__DWH.h_transactions dt ON dt.operation_id = st.operation_id
WHERE
    st.transaction_dt::DATE = '2022-10-01'
LIMIT
    1 OVER (PARTITION BY hk_transaction_id, status_dt, status ORDER BY st.id DESC);