from typing import TypeVar, Callable, List, Tuple, Optional, Iterable
from datetime import datetime, timezone
from sqlalchemy.exc import IntegrityError
from swpt_creditors.extensions import db
from sqlalchemy.orm import exc, Load
from swpt_creditors.models import (
    Creditor, PendingLogEntry, LogEntry, ConfigureAccountSignal,
    Account, AccountKnowledge, AccountExchange, AccountDisplay, AccountData,
    MIN_INT64, MAX_INT64, DEFAULT_CREDITOR_STATUS, DEFAULT_CONFIG_FLAGS, DEFAULT_NEGLIGIBLE_AMOUNT,
)
from .common import allow_update, get_paths_and_types, ACCOUNT_DATA_CONFIG_RELATED_COLUMNS
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


@atomic
def create_new_creditor(creditor_id: int, activate: bool = False) -> Creditor:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    creditor = Creditor(creditor_id=creditor_id, status=DEFAULT_CREDITOR_STATUS)
    creditor.is_active = activate

    db.session.add(creditor)
    try:
        db.session.flush()
    except IntegrityError:
        raise errors.CreditorExists()

    return creditor


@atomic
def activate_creditor(creditor_id: int) -> None:
    creditor = Creditor.lock_instance(creditor_id)
    if creditor:
        creditor.is_active = True


@atomic
def update_creditor(creditor_id: int, *, latest_update_id: int) -> Creditor:
    assert 1 <= latest_update_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    try:
        perform_update = allow_update(creditor, 'creditor_latest_update_id', latest_update_id, {})
    except errors.AlreadyUpToDate:
        return creditor

    creditor.creditor_latest_update_ts = current_ts
    perform_update()

    paths, types = get_paths_and_types()
    _add_log_entry(
        creditor,
        object_type=types.creditor,
        object_uri=paths.creditor(creditorId=creditor_id),
        object_update_id=creditor.creditor_latest_update_id,
        added_at_ts=current_ts,
    )

    return creditor


@atomic
def get_creditor(creditor_id: int, lock: bool = False) -> Optional[Creditor]:
    if lock:
        creditor = Creditor.lock_instance(creditor_id)
    else:
        creditor = Creditor.get_instance(creditor_id)

    if creditor and creditor.is_active and creditor.deactivated_at_date is None:
        return creditor


@atomic
def get_creditors_with_pending_log_entries() -> Iterable[int]:
    return set(t[0] for t in db.session.query(PendingLogEntry.creditor_id).all())


@atomic
def process_pending_log_entries(creditor_id: int) -> None:
    creditor = Creditor.lock_instance(creditor_id)
    if creditor is None:
        return

    pending_log_entries = PendingLogEntry.query.\
        filter_by(creditor_id=creditor_id).\
        order_by(PendingLogEntry.pending_entry_id).\
        with_for_update().\
        all()

    if pending_log_entries:
        paths, types = get_paths_and_types()
        for entry in pending_log_entries:
            _add_log_entry(
                creditor,
                object_type=entry.object_type,
                object_uri=entry.object_uri,
                object_update_id=entry.object_update_id,
                added_at_ts=entry.added_at_ts,
                is_deleted=entry.is_deleted,
                data=entry.data,
            )

            # NOTE: When a transfer has been initiated or deleted, the
            # creditor's list of transfers is undated too, and the
            # client should be informed about this. This hack is
            # necessary, because the update of the creditor's list of
            # transfers requires the `Creditor` table row to be
            # locked.
            if entry.object_type == types.transfer and (entry.is_created or entry.is_deleted):
                creditor.transfer_list_latest_update_id += 1
                creditor.transfer_list_latest_update_ts = entry.added_at_ts
                _add_log_entry(
                    creditor,
                    object_type=types.transfer_list,
                    object_uri=paths.transfer_list(creditorId=creditor_id),
                    object_update_id=creditor.transfer_list_latest_update_id,
                    added_at_ts=creditor.transfer_list_latest_update_ts,
                )

            db.session.delete(entry)


@atomic
def get_creditor_log_entries(creditor_id: int, *, count: int = 1, prev: int = 0) -> Tuple[List[LogEntry], int]:
    assert count >= 1
    assert 0 <= prev <= MAX_INT64

    last_log_entry_id = db.session.\
        query(Creditor.last_log_entry_id).\
        filter(Creditor.creditor_id == creditor_id).\
        scalar()

    if last_log_entry_id is None:
        raise errors.CreditorDoesNotExist()

    log_entries = LogEntry.query.\
        filter(LogEntry.creditor_id == creditor_id).\
        filter(LogEntry.entry_id > prev).\
        order_by(LogEntry.entry_id).\
        limit(count).\
        all()

    return log_entries, last_log_entry_id


@atomic
def has_account(creditor_id: int, debtor_id: int) -> bool:
    account_query = Account.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    return db.session.query(account_query.exists()).scalar()


@atomic
def create_new_account(creditor_id: int, debtor_id: int) -> Account:
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    if has_account(creditor_id, debtor_id):
        raise errors.AccountExists()

    return _create_new_account(creditor, debtor_id, current_ts)


@atomic
def delete_account(creditor_id: int, debtor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    query = db.session.\
        query(AccountData, Creditor).\
        join(Creditor, Creditor.creditor_id == AccountData.creditor_id).\
        filter(AccountData.creditor_id == creditor_id, AccountData.debtor_id == debtor_id).\
        with_for_update(of=Creditor).\
        options(Load(AccountData).load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS))

    try:
        data, creditor = query.one()
    except exc.NoResultFound:
        raise errors.AccountDoesNotExist()

    if not (data.is_deletion_safe or data.allow_unsafe_deletion):
        raise errors.UnsafeAccountDeletion()

    pegged_accounts_query = AccountExchange.query.filter_by(creditor_id=creditor_id, peg_debtor_id=debtor_id)
    if db.session.query(pegged_accounts_query.exists()).scalar():
        raise errors.ForbiddenPegDeletion()

    with db.retry_on_integrity_error():
        Account.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id).delete(synchronize_session=False)

    # NOTE: When the account gets deleted, all its related objects
    # will be deleted too. Also, the deleted account will disappear
    # from the list of accounts. Therefore, we need to write a bunch
    # of events to the log, so as to inform the client.
    creditor.account_list_latest_update_id += 1
    creditor.account_list_latest_update_ts = current_ts
    paths, types = get_paths_and_types()
    _add_log_entry(
        creditor,
        object_type=types.account_list,
        object_uri=paths.account_list(creditorId=creditor_id),
        object_update_id=creditor.account_list_latest_update_id,
        added_at_ts=current_ts,
    )
    deletion_events = [
        (types.account, paths.account(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_config, paths.account_config(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_info, paths.account_info(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_ledger, paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_display, paths.account_display(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_exchange, paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_knowledge, paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id)),
    ]
    for object_type, object_uri in deletion_events:
        _add_log_entry(
            creditor,
            object_type=object_type,
            object_uri=object_uri,
            added_at_ts=current_ts,
            is_deleted=True,
        )


@atomic
def get_creditor_debtor_ids(creditor_id: int, count: int = 1, prev: int = None) -> List[int]:
    assert count >= 1
    assert prev is None or MIN_INT64 <= prev <= MAX_INT64

    query = db.session.\
        query(Account.debtor_id).\
        filter(Account.creditor_id == creditor_id).\
        order_by(Account.debtor_id)

    if prev is not None:
        query = query.filter(Account.debtor_id > prev)

    return [t[0] for t in query.limit(count).all()]


def _create_new_account(creditor: Creditor, debtor_id: int, current_ts: datetime) -> Account:
    creditor_id = creditor.creditor_id
    paths, types = get_paths_and_types()
    _add_log_entry(
        creditor,
        object_type=types.account,
        object_uri=paths.account(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=1,
        added_at_ts=current_ts,
    )

    # NOTE: The new account will appear in the creditor's list of accounts.
    creditor.account_list_latest_update_id += 1
    creditor.account_list_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.account_list,
        object_uri=paths.account_list(creditorId=creditor_id),
        object_update_id=creditor.account_list_latest_update_id,
        added_at_ts=current_ts,
    )

    account = Account(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        created_at_ts=current_ts,
        knowledge=AccountKnowledge(latest_update_ts=current_ts),
        exchange=AccountExchange(latest_update_ts=current_ts),
        display=AccountDisplay(latest_update_ts=current_ts),
        data=AccountData(
            last_config_ts=current_ts,
            last_config_seqnum=0,
            config_latest_update_ts=current_ts,
            info_latest_update_ts=current_ts,
            ledger_latest_update_ts=current_ts,
        ),
        latest_update_ts=current_ts,
    )
    db.session.add(account)

    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=current_ts,
        seqnum=0,
        negligible_amount=DEFAULT_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
        config='',
    ))

    return account


def _add_log_entry(
        creditor: Creditor,
        *,
        added_at_ts: datetime,
        object_type: str,
        object_uri: str,
        object_update_id: int = None,
        is_deleted: bool = False,
        data: dict = None) -> None:

    db.session.add(LogEntry(
        creditor_id=creditor.creditor_id,
        entry_id=creditor.generate_log_entry_id(),
        object_type=object_type,
        object_uri=object_uri,
        object_update_id=object_update_id,
        added_at_ts=added_at_ts,
        is_deleted=is_deleted,
        data=data,
    ))