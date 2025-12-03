
from logging import Logger
from datetime import datetime

from lib.kafka_connect import KafkaConsumer
from stg_loader.repository import StgRepository

class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 stg_repository: StgRepository,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = 1000

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        # self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            # self._logger.info(f"{datetime.utcnow()}: Message received")

            # self._logger.info(f"MSG: {msg}")

            if msg["object_type"] == "CURRENCY":
                self.parse_currency(msg)
            if msg["object_type"] == "TRANSACTION":
                self.parse_transaction(msg)

            self._logger.info(f"{datetime.utcnow()}. Message Sent")
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def parse_currency(self, msg: dict):
        payload = msg["payload"]
        date_update = payload["date_update"]
        currency_code = payload["currency_code"]
        currency_code_with = payload["currency_code_with"]
        currency_code_div = payload["currency_with_div"]
        self._stg_repository.сurrencies_source_insert(
            date_update=date_update,
            currency_code=currency_code,
            currency_code_with=currency_code_with,
            currency_code_div=currency_code_div
        )

    def parse_transaction(self, msg: dict):
        payload = msg["payload"]
        operation_id = payload["operation_id"]
        account_number_from = payload["account_number_from"]
        account_number_to = payload["account_number_to"]
        currency_code = payload["currency_code"]
        country = payload["country"]
        status = payload["status"]
        transaction_type = payload["transaction_type"]
        amount = payload["amount"]
        transaction_dt = payload["transaction_dt"]
        self._stg_repository.transactions_source_insert(
            operation_id=operation_id,
            account_number_from=account_number_from,
            account_number_to=account_number_to,
            currency_code=currency_code,
            country=country,
            status=status,
            transaction_type=transaction_type,
            amount=amount,
            transaction_dt=transaction_dt
        )