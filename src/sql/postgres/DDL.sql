--drop table if EXISTS DE_FINAL_PROJECT_STAGING.currencies_source;

CREATE TABLE IF NOT EXISTS DE_FINAL_PROJECT_STAGING.currencies_source (
    id serial PRIMARY KEY,
    date_update timestamp NOT null,
    currency_code int NOT null,
    currency_code_with int NOT null,
    currency_code_div numeric(5,2) NOT null CHECK (currency_code_div>=0),
    UNIQUE(date_update,currency_code,currency_code_with,currency_code_div)
);


--drop table if EXISTS DE_FINAL_PROJECT_STAGING.transactions_source;

CREATE TABLE IF NOT EXISTS DE_FINAL_PROJECT_STAGING.transactions_source (
    id serial PRIMARY key,
    operation_id UUID NOT null,
    account_number_from int not null,
    account_number_to int NOT NULL,
    currency_code int NOT null,
    country VARCHAR NOT null,
    status VARCHAR NOT null,
    transaction_type VARCHAR NOT null,
    amount int NOT null CHECK (amount>=0),
    transaction_dt timestamp NOT null,
    UNIQUE(operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt)
);
