INSERT INTO
    VT251201B6F661__DWH.l_account_transaction(hk_l_account_transaction, hk_transaction_id, hk_account_from_id, hk_account_to_id, load_dt, load_src)
SELECT
    HASH(daf.hk_account_id, dat.hk_account_id, dt.hk_transaction_id) AS hk_l_account_transaction,
    dt.hk_transaction_id AS hk_transaction_id,
    daf.hk_account_id AS hk_account_from_id,
    dat.hk_account_id AS hk_account_to_id,
    '{{ ds }}' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.transactions_source st
LEFT JOIN
    VT251201B6F661__DWH.h_accounts daf ON daf.account_number = st.account_number_from
LEFT JOIN
    VT251201B6F661__DWH.h_accounts dat ON dat.account_number = st.account_number_to
LEFT JOIN
    VT251201B6F661__DWH.h_transactions dt ON dt.operation_id = st.operation_id
WHERE
    st.transaction_dt::DATE = '{{ ds }}'
    AND (HASH(daf.hk_account_id, dat.hk_account_id, dt.hk_transaction_id) NOT IN (SELECT hk_l_account_transaction FROM VT251201B6F661__DWH.l_account_transaction))
LIMIT
    1 OVER (PARTITION BY dt.hk_transaction_id, daf.hk_account_id, dat.hk_account_id ORDER BY st.id DESC);