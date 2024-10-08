from datetime import datetime, date, timezone, timedelta
from typing import TypeVar, Callable, Tuple, List, Optional
from flask import current_app
from sqlalchemy.sql.expression import func, text
from sqlalchemy.orm import exc, Load
from swpt_pythonlib.utils import Seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    AccountData,
    ConfigureAccountSignal,
    UpdatedLedgerSignal,
    LogEntry,
    PendingLogEntry,
    PendingLedgerUpdate,
    LedgerEntry,
    CommittedTransfer,
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
    MIN_INT64,
    MAX_INT64,
    uid_seq,
)
from .common import (
    ACCOUNT_DATA_LEDGER_RELATED_COLUMNS,
    LOAD_ONLY_CONFIG_RELATED_COLUMNS,
    LOAD_ONLY_INFO_RELATED_COLUMNS,
)
from .common import contain_principal_overflow
from .accounts import _insert_info_update_pending_log_entry
from .transfers import ensure_pending_ledger_update

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5
HUGE_INTERVAL = timedelta(days=500000)

CALL_PROCESS_PENDING_LEDGER_UPDATE = text(
    "SELECT process_pending_ledger_update(:creditor_id, :debtor_id, "
    ":max_delay)"
)


@atomic
def process_rejected_config_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    config_ts: datetime,
    config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    rejection_code: str
) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    data = (
        AccountData.query.filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            last_config_ts=config_ts,
            last_config_seqnum=config_seqnum,
            config_flags=config_flags,
            config_data=config_data,
            config_error=None,
        )
        .filter(
            func.abs(AccountData.negligible_amount - negligible_amount)
            <= EPS * negligible_amount
        )
        .with_for_update(key_share=True)
        .options(LOAD_ONLY_CONFIG_RELATED_COLUMNS)
        .one_or_none()
    )

    if data:
        data.config_error = rejection_code
        _insert_info_update_pending_log_entry(data, current_ts)


@atomic
def process_account_update_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    creation_date: date,
    last_change_ts: datetime,
    last_change_seqnum: int,
    principal: int,
    interest: float,
    interest_rate: float,
    last_interest_rate_change_ts: datetime,
    transfer_note_max_bytes: int,
    last_config_ts: datetime,
    last_config_seqnum: int,
    negligible_amount: float,
    config_flags: int,
    config_data: str,
    account_id: str,
    debtor_info_iri: Optional[str],
    debtor_info_content_type: Optional[str],
    debtor_info_sha256: Optional[bytes],
    last_transfer_number: int,
    last_transfer_committed_at: datetime,
    ts: datetime,
    ttl: int
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    data = (
        AccountData.query.filter_by(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
        .with_for_update(key_share=True)
        .one_or_none()
    )
    if data is None:
        _discard_orphaned_account(
            creditor_id, debtor_id, config_flags, negligible_amount
        )
        return

    if ts > data.last_heartbeat_ts:
        data.last_heartbeat_ts = min(ts, current_ts)

    prev_event = (
        data.creation_date,
        data.last_change_ts,
        Seqnum(data.last_change_seqnum),
    )
    this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
    if this_event <= prev_event:
        return

    assert creation_date >= data.creation_date
    is_new_server_account = creation_date > data.creation_date
    is_account_id_changed = account_id != data.account_id
    is_config_effectual = (
        last_config_ts == data.last_config_ts
        and last_config_seqnum == data.last_config_seqnum
        and config_flags == data.config_flags
        and config_data == data.config_data
        and abs(data.negligible_amount - negligible_amount)
        <= EPS * negligible_amount
    )
    config_error = None if is_config_effectual else data.config_error
    is_info_updated = (
        data.is_deletion_safe
        or data.account_id != account_id
        or abs(data.interest_rate - interest_rate) > EPS * interest_rate
        or data.last_interest_rate_change_ts != last_interest_rate_change_ts
        or data.transfer_note_max_bytes != transfer_note_max_bytes
        or data.debtor_info_iri != debtor_info_iri
        or data.debtor_info_content_type != debtor_info_content_type
        or data.debtor_info_sha256 != debtor_info_sha256
        or data.config_error != config_error
    )

    data.has_server_account = True
    data.creation_date = creation_date
    data.last_change_ts = last_change_ts
    data.last_change_seqnum = last_change_seqnum
    data.principal = principal
    data.interest = interest
    data.interest_rate = interest_rate
    data.last_interest_rate_change_ts = last_interest_rate_change_ts
    data.transfer_note_max_bytes = transfer_note_max_bytes
    data.account_id = account_id
    data.debtor_info_iri = debtor_info_iri
    data.debtor_info_content_type = debtor_info_content_type
    data.debtor_info_sha256 = debtor_info_sha256
    data.last_transfer_number = last_transfer_number
    data.last_transfer_committed_at = last_transfer_committed_at
    data.is_config_effectual = is_config_effectual
    data.config_error = config_error

    if is_info_updated:
        _insert_info_update_pending_log_entry(data, current_ts)

    if is_new_server_account or is_account_id_changed:
        if is_new_server_account:
            data.ledger_pending_transfer_ts = None
            ledger_principal = 0
            ledger_last_transfer_number = 0
        else:  # pragma: no cover
            # When the `account_id` field is changed, we should send a
            # corresponding `UpdatedLedgerSignal` message. To do this
            # consistently with the Web API, first we need to add a ledger
            # update log entry, even when the ledger did not really change.
            assert is_account_id_changed
            ledger_principal = data.ledger_principal
            ledger_last_transfer_number = data.ledger_last_transfer_number

        log_entry = _update_ledger(
            data=data,
            transfer_number=ledger_last_transfer_number,
            acquired_amount=0,
            principal=ledger_principal,
            current_ts=current_ts,
            always_insert_ledger_update_log_entry=True,
        )
        if log_entry:
            db.session.add(
                UpdatedLedgerSignal(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    update_id=data.ledger_latest_update_id,
                    account_id=data.account_id,
                    creation_date=data.creation_date,
                    principal=ledger_principal,
                    last_transfer_number=ledger_last_transfer_number,
                    ts=current_ts,
                )
            )
            db.session.add(log_entry)
            db.session.scalar(uid_seq)

        ensure_pending_ledger_update(data.creditor_id, data.debtor_id)


@atomic
def process_account_purge_signal(
    *, debtor_id: int, creditor_id: int, creation_date: date
) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    data = (
        AccountData.query.filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            has_server_account=True,
        )
        .filter(AccountData.creation_date <= creation_date)
        .with_for_update(key_share=True)
        .options(LOAD_ONLY_INFO_RELATED_COLUMNS)
        .one_or_none()
    )

    if data:
        data.has_server_account = False
        data.principal = 0
        data.interest = 0.0
        _insert_info_update_pending_log_entry(data, current_ts)


@atomic
def get_pending_ledger_updates(max_count: int = None) -> List[Tuple[int, int]]:
    query = db.session.query(
        PendingLedgerUpdate.creditor_id, PendingLedgerUpdate.debtor_id
    )
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_pending_ledger_update(
    creditor_id: int, debtor_id: int, *, burst_count: int, max_delay: timedelta
) -> bool:
    """Try to add pending committed transfers to the account's ledger.

    This function will not try to process more than `burst_count`
    transfers. When some legible committed transfers remained
    unprocessed, `False` will be returned. In this case the function
    should be called again, and again, until it returns `True`.

    When one or more `AccountTransfer` messages have been lost, after
    some time (determined by the `max_delay` attribute), the account's
    ledger will be automatically "repaired", and the lost transfers
    skipped.

    """

    if current_app.config["APP_USE_PGPLSQL_FUNCTIONS"]:  # pragma: no cover
        db.session.execute(
            CALL_PROCESS_PENDING_LEDGER_UPDATE,
            {
                "creditor_id": creditor_id,
                "debtor_id": debtor_id,
                "max_delay": max_delay,
            },
        )
        # NOTE: The PG/PLSQL function ignores the `burst_count`, and
        # processes all pending committed transfers at once.
        return True

    current_ts = datetime.now(tz=timezone.utc)

    pending_ledger_update = (
        db.session.query(PendingLedgerUpdate)
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if pending_ledger_update is None:
        return True

    data = (
        db.session.query(AccountData)
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update(key_share=True)
        .options(
            Load(AccountData).load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS)
        )
        .one()
    )
    log_entry = None
    committed_at_cutoff = current_ts - max_delay
    transfers = _get_sorted_pending_transfers(data, burst_count)

    for (
        previous_transfer_number,
        transfer_number,
        acquired_amount,
        principal,
        committed_at,
    ) in transfers:
        if (
            previous_transfer_number != data.ledger_last_transfer_number
            and committed_at >= committed_at_cutoff
        ):
            data.ledger_pending_transfer_ts = committed_at
            is_done = True
            break
        log_entry = (
            _update_ledger(
                data=data,
                transfer_number=transfer_number,
                acquired_amount=acquired_amount,
                principal=principal,
                current_ts=current_ts,
            )
            or log_entry
        )
    else:
        data.ledger_pending_transfer_ts = None
        is_done = len(transfers) < burst_count

    if is_done:
        log_entry = (
            _fix_missing_last_transfer_if_necessary(
                data, max_delay, current_ts
            )
            or log_entry
        )
        db.session.delete(pending_ledger_update)

    if log_entry:
        db.session.add(UpdatedLedgerSignal(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            update_id=data.ledger_latest_update_id,
            account_id=data.account_id,
            creation_date=data.creation_date,
            principal=data.ledger_principal,
            last_transfer_number=data.ledger_last_transfer_number,
            ts=current_ts,
        ))
        db.session.add(log_entry)

        while db.session.scalar(uid_seq) < data.ledger_latest_update_id:
            pass  # pragma: no cover

    return is_done


def _get_sorted_pending_transfers(
    data: AccountData, max_count: int
) -> List[Tuple]:
    return (
        db.session.query(
            CommittedTransfer.previous_transfer_number,
            CommittedTransfer.transfer_number,
            CommittedTransfer.acquired_amount,
            CommittedTransfer.principal,
            CommittedTransfer.committed_at,
        )
        .filter(
            CommittedTransfer.creditor_id == data.creditor_id,
            CommittedTransfer.debtor_id == data.debtor_id,
            CommittedTransfer.creation_date == data.creation_date,
            CommittedTransfer.transfer_number
            > data.ledger_last_transfer_number,
        )
        .order_by(CommittedTransfer.transfer_number)
        .limit(max_count)
        .all()
    )


def _fix_missing_last_transfer_if_necessary(
    data: AccountData, max_delay: timedelta, current_ts: datetime
) -> Optional[PendingLogEntry]:
    has_no_pending_transfers = data.ledger_pending_transfer_ts is None
    last_transfer_is_missing = (
        data.last_transfer_number > data.ledger_last_transfer_number
    )
    last_transfer_is_old = (
        data.last_transfer_committed_at < current_ts - max_delay
    )

    if (
        has_no_pending_transfers
        and last_transfer_is_missing
        and last_transfer_is_old
    ):
        return _update_ledger(
            data=data,
            transfer_number=data.last_transfer_number,
            acquired_amount=0,
            principal=data.principal,
            current_ts=current_ts,
        )


def _discard_orphaned_account(
    creditor_id: int,
    debtor_id: int,
    config_flags: int,
    negligible_amount: float,
) -> None:
    scheduled_for_deletion_flag = (
        AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT
    is_already_discarded = (
        config_flags & scheduled_for_deletion_flag
        and negligible_amount >= safely_huge_amount
    )

    if not is_already_discarded:
        db.session.add(
            ConfigureAccountSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS
                | scheduled_for_deletion_flag,
            )
        )


def _update_ledger(
    data: AccountData,
    transfer_number: int,
    acquired_amount: int,
    principal: int,
    current_ts: datetime,
    always_insert_ledger_update_log_entry: bool = False,
) -> Optional[PendingLogEntry]:
    should_insert_ledger_update_log_entry = (
        _make_correcting_ledger_entry_if_necessary(
            data=data,
            acquired_amount=acquired_amount,
            principal=principal,
            current_ts=current_ts,
        )
    )

    if acquired_amount != 0:
        data.ledger_last_entry_id += 1
        db.session.add(
            LedgerEntry(
                creditor_id=data.creditor_id,
                debtor_id=data.debtor_id,
                entry_id=data.ledger_last_entry_id,
                acquired_amount=acquired_amount,
                principal=principal,
                added_at=current_ts,
                creation_date=data.creation_date,
                transfer_number=transfer_number,
            )
        )
        should_insert_ledger_update_log_entry = True

    assert (
        should_insert_ledger_update_log_entry
        or data.ledger_principal == principal
    )
    data.ledger_principal = principal
    data.ledger_last_transfer_number = transfer_number

    if (
        should_insert_ledger_update_log_entry
        or always_insert_ledger_update_log_entry
    ):
        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts

        return PendingLogEntry(
            creditor_id=data.creditor_id,
            added_at=current_ts,
            object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER,
            debtor_id=data.debtor_id,
            object_update_id=data.ledger_latest_update_id,
            data_principal=principal,
            data_next_entry_id=data.ledger_last_entry_id + 1,
        )


def _make_correcting_ledger_entry_if_necessary(
    data: AccountData,
    acquired_amount: int,
    principal: int,
    current_ts: datetime,
) -> bool:
    made_correcting_ledger_entry = False
    previous_principal = principal - acquired_amount

    if MIN_INT64 <= previous_principal <= MAX_INT64:
        ledger_principal = data.ledger_principal
        correction_amount = previous_principal - ledger_principal

        while correction_amount != 0:
            safe_correction_amount = contain_principal_overflow(
                correction_amount
            )
            correction_amount -= safe_correction_amount
            ledger_principal += safe_correction_amount

            data.ledger_last_entry_id += 1
            db.session.add(
                LedgerEntry(
                    creditor_id=data.creditor_id,
                    debtor_id=data.debtor_id,
                    entry_id=data.ledger_last_entry_id,
                    acquired_amount=safe_correction_amount,
                    principal=ledger_principal,
                    added_at=current_ts,
                )
            )
            made_correcting_ledger_entry = True

    return made_correcting_ledger_entry
