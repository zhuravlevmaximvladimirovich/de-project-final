INSERT INTO
    VT251201B6F661__DWH.h_transactions(hk_transaction_id, operation_id, transaction_type, amount, transaction_dt, load_dt, load_src)
SELECT
    HASH(operation_id) AS hk_operation_id,
    operation_id,
    transaction_type,
    amount,
    transaction_dt,
    '2022-10-01' AS load_dt,
    'stg-service-de-final-project' AS load_src
FROM
    VT251201B6F661__STAGING.transactions_source
WHERE
    transaction_dt::date = '2022-10-01' AND (HASH(operation_id) NOT IN (SELECT hk_transaction_id FROM VT251201B6F661__DWH.h_transactions))
LIMIT
    1 OVER (PARTITION BY operation_id ORDER BY id DESC);