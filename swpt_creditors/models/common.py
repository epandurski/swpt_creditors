from __future__ import annotations
import json
from datetime import datetime, timezone
from flask import current_app
from swpt_creditors.extensions import db, publisher
from swpt_pythonlib import rabbitmq

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
ROOT_CREDITOR_ID = 0
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
T_INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
DATE0 = TS0.date()
PIN_REGEX = r"^[0-9]{4,10}$"
TRANSFER_NOTE_MAX_BYTES = 500
TRANSFER_NOTE_FORMAT_REGEX = r"^[0-9A-Za-z.-]{0,8}$"
CONFIG_DATA_MAX_BYTES = 2000

CT_DIRECT = "direct"


def get_now_utc():
    return datetime.now(tz=timezone.utc)


def is_valid_creditor_id(creditor_id: int, match_parent=False) -> bool:
    sharding_realm = current_app.config["SHARDING_REALM"]
    min_creditor_id = current_app.config["MIN_CREDITOR_ID"]
    max_creditor_id = current_app.config["MAX_CREDITOR_ID"]
    return (
        min_creditor_id <= creditor_id <= max_creditor_id
        and sharding_realm.match(creditor_id, match_parent=match_parent)
    )


class Signal(db.Model):
    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert all(isinstance(obj, cls) for obj in objects)
        messages = (obj._create_message() for obj in objects)
        publisher.publish_messages([m for m in messages if m is not None])

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):  # pragma: no cover
        data = self.__marshmallow_schema__.dump(self)
        message_type = data["type"]
        creditor_id = data["creditor_id"]
        debtor_id = data["debtor_id"]

        if (
                message_type != "RejectedConfig"
                and not is_valid_creditor_id(creditor_id)
        ):
            if current_app.config[
                "DELETE_PARENT_SHARD_RECORDS"
            ] and is_valid_creditor_id(creditor_id, match_parent=True):
                # This message most probably is a left-over from the
                # previous splitting of the parent shard into children
                # shards. Therefore we should just ignore it.
                return None
            raise RuntimeError(
                "The agent is not responsible for this creditor."
            )

        headers = {
            "message-type": message_type,
            "debtor-id": debtor_id,
            "creditor-id": creditor_id,
        }
        if "coordinator_id" in data:
            headers["coordinator-id"] = data["coordinator_id"]
            headers["coordinator-type"] = data["coordinator_type"]

        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id="swpt_creditors",
            content_type="application/json",
            type=message_type,
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf8")

        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=properties,
            mandatory=message_type == "FinalizeTransfer",
        )

    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
