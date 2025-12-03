from datetime import datetime

from lib.pg import PgConnect

class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def transactions_source_insert(self,
                            operation_id: str,
                            account_number_from: int,
                            account_number_to: int,
                            currency_code: int,
                            country: str,
                            status: str,
                            transaction_type: str,
                            amount: int,
                            transaction_dt: datetime,
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO DE_FINAL_PROJECT_STAGING.transactions_source(
                    operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt
                    )
                    VALUES (
                    %(operation_id)s, %(account_number_from)s, %(account_number_to)s,%(currency_code)s,
                    %(country)s, %(status)s, %(transaction_type)s, %(amount)s, %(transaction_dt)s
                    )
                    ON CONFLICT (operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt)
                    DO NOTHING;
                    """,
                    {
                        'operation_id': operation_id,
                        'account_number_from': account_number_from,
                        'account_number_to': account_number_to,
                        'currency_code': currency_code,
                        'country': country,
                        'status': status,
                        'transaction_type': transaction_type,
                        'amount': amount,
                        'transaction_dt': transaction_dt,
                    }
                )

    def сurrencies_source_insert(self,
                            date_update: datetime,
                            currency_code: int,
                            currency_code_with: int,
                            currency_code_div: float
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO DE_FINAL_PROJECT_STAGING.сurrencies_source(date_update, currency_code, currency_code_with, currency_code_div)
                    VALUES (%(date_update)s, %(currency_code)s, %(currency_code_with)s, %(currency_code_div)s)
                    ON CONFLICT (date_update, currency_code, currency_code_with, currency_code_div)
                    DO NOTHING;
                    """,
                    {
                        'date_update': date_update,
                        'currency_code': currency_code,
                        'currency_code_with': currency_code_with,
                        'currency_code_div': currency_code_div
                    }
                )