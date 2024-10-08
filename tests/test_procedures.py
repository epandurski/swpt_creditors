import pytest
import time
from datetime import date, timedelta, datetime, timezone
from uuid import UUID
from swpt_pythonlib.utils import i64_to_u64
from swpt_creditors import procedures as p
from swpt_creditors import models
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    Creditor,
    AccountData,
    ConfigureAccountSignal,
    LogEntry,
    CommittedTransfer,
    PendingLedgerUpdate,
    PrepareTransferSignal,
    RunningTransfer,
    FinalizeTransferSignal,
    RejectedConfigSignal,
)

D_ID = -1
C_ID = 4294967296
TEST_UUID = UUID("123e4567-e89b-12d3-a456-426655440000")
TEST_UUID2 = UUID("123e4567-e89b-12d3-a456-426655440001")


@pytest.fixture(params=[2, 1000000])
def burst_count(request):
    return request.param


@pytest.fixture
def creditor(db_session):
    creditor = p.reserve_creditor(C_ID)
    p.activate_creditor(C_ID, str(creditor.reservation_id))
    return creditor


@pytest.fixture
def account(creditor):
    return p.create_new_account(C_ID, D_ID)


def test_activate_new_creditor(db_session):
    creditor = p.reserve_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert not creditor.is_activated
    assert len(Creditor.query.all()) == 1
    with pytest.raises(p.CreditorExists):
        p.reserve_creditor(C_ID)

    assert not p.get_active_creditor(C_ID)
    with pytest.raises(p.InvalidReservationId):
        p.activate_creditor(C_ID, "-123")
    p.activate_creditor(C_ID, str(creditor.reservation_id))
    creditor = p.get_active_creditor(C_ID)
    assert creditor
    assert creditor.is_activated

    with pytest.raises(p.CreditorExists):
        p.reserve_creditor(C_ID)


def test_create_account(creditor):
    with pytest.raises(p.CreditorDoesNotExist):
        p.create_new_account(666, D_ID)

    account = p.create_new_account(C_ID, D_ID)
    assert account
    assert account.creditor_id == C_ID
    assert account.debtor_id == D_ID


def test_deactivate_creditor(account):
    assert p.get_active_creditor(C_ID)
    assert len(models.Account.query.all()) == 1
    assert len(models.PinInfo.query.all()) == 1
    p.deactivate_creditor(C_ID)
    assert p.get_active_creditor(C_ID) is None
    assert len(models.Account.query.all()) == 0
    assert len(models.PinInfo.query.all()) == 0


def test_delete_account_without_debtor_name(account, current_ts):
    p.delete_account(C_ID, D_ID)
    assert not p.get_account(C_ID, D_ID)


def test_delete_account(account, current_ts):
    with pytest.raises(p.AccountDoesNotExist):
        p.delete_account(C_ID, 1234)

    display = p.get_account_display(C_ID, D_ID)
    assert display is not None
    p.update_account_display(
        C_ID,
        D_ID,
        debtor_name="test_name",
        amount_divisor=1.0,
        decimal_places=0,
        unit="USD",
        known_debtor=True,
        latest_update_id=display.latest_update_id + 1,
    )
    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "last_change_ts": current_ts,
        "last_change_seqnum": 1,
        "principal": 1000,
        "interest": 0.0,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": current_ts,
        "last_transfer_number": 1,
        "last_transfer_committed_at": current_ts,
        "last_config_ts": current_ts,
        "last_config_seqnum": 1,
        "creation_date": date(2020, 1, 15),
        "negligible_amount": 0.0,
        "ts": current_ts,
        "ttl": 1000000,
        "account_id": str(C_ID),
        "config_data": "",
        "config_flags": 0,
        "debtor_info_iri": "http://example.com",
        "debtor_info_content_type": None,
        "debtor_info_sha256": None,
        "transfer_note_max_bytes": 500,
    }

    assert len(models.UpdatedLedgerSignal.query.all()) == 0
    p.process_account_update_signal(**params)
    assert len(models.UpdatedLedgerSignal.query.all()) == 1
    with pytest.raises(p.UnsafeAccountDeletion):
        p.delete_account(C_ID, D_ID)

    assert len(models.UpdatedFlagsSignal.query.all()) == 0
    latest_update_id = p.get_account_config(C_ID, D_ID).config_latest_update_id
    p.update_account_config(
        C_ID,
        D_ID,
        is_scheduled_for_deletion=True,
        negligible_amount=0.0,
        allow_unsafe_deletion=False,
        config_data="",
        latest_update_id=latest_update_id + 1,
    )
    ufs = models.UpdatedFlagsSignal.query.one()
    assert ufs.creditor_id == C_ID
    assert ufs.debtor_id == D_ID
    assert ufs.config_flags == (
        models.DEFAULT_CONFIG_FLAGS
        | AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    assert isinstance(ufs.ts, date)

    config = p.get_account_config(C_ID, D_ID)
    params["last_change_seqnum"] += 1
    params["last_config_ts"] = config.last_config_ts
    params["last_config_seqnum"] = config.last_config_seqnum
    params["negligible_amount"] = config.negligible_amount
    params["config_flags"] = config.config_flags
    p.process_account_update_signal(**params)

    assert len(models.UpdatedLedgerSignal.query.all()) == 1
    assert len(models.UpdatedPolicySignal.query.all()) == 0
    p.process_account_purge_signal(
        debtor_id=D_ID, creditor_id=C_ID, creation_date=date(2020, 1, 15)
    )
    p.delete_account(C_ID, D_ID)
    assert not p.get_account(C_ID, D_ID)
    assert len(models.UpdatedLedgerSignal.query.all()) == 2

    uls = models.UpdatedLedgerSignal.query.filter_by(account_id='').one()
    assert uls.creditor_id == C_ID
    assert uls.debtor_id == D_ID
    assert uls.creation_date == models.DATE0
    assert uls.principal == 0
    assert uls.last_transfer_number == 0
    assert isinstance(uls.ts, date)

    ups = models.UpdatedPolicySignal.query.one()
    assert ups.creditor_id == C_ID
    assert ups.debtor_id == D_ID
    assert ups.policy_name is None
    assert ups.min_principal == models.MIN_INT64
    assert ups.max_principal == models.MAX_INT64
    assert ups.peg_exchange_rate is None
    assert ups.peg_debtor_id is None
    assert isinstance(ups.ts, date)

    assert len(models.UpdatedLedgerSignal.query.all()) == 2
    ufs = models.UpdatedFlagsSignal.query.filter_by(
        config_flags=models.DEFAULT_CONFIG_FLAGS
    ).one()
    assert ufs.creditor_id == C_ID
    assert ufs.debtor_id == D_ID
    assert isinstance(ufs.ts, date)


def test_process_account_update_signal(account):
    AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).update(
        {
            "ledger_principal": 1001,
            "ledger_last_entry_id": 88,
            "ledger_last_transfer_number": 888,
        }
    )

    def get_data():
        return AccountData.query.filter_by(
            creditor_id=C_ID, debtor_id=D_ID
        ).one()

    ad = get_data()
    assert not ad.is_config_effectual
    assert ad.ledger_principal == 1001
    assert ad.ledger_last_entry_id == 88
    assert ad.negligible_amount == models.DEFAULT_NEGLIGIBLE_AMOUNT
    assert ad.config_flags == models.DEFAULT_CONFIG_FLAGS
    assert ad.last_change_seqnum == 0
    assert ad.config_error is None
    last_ts = ad.last_config_ts
    last_seqnum = ad.last_config_seqnum
    negligible_amount = ad.negligible_amount
    config_flags = ad.config_flags
    last_heartbeat_ts = ad.last_heartbeat_ts
    creation_date = date(2020, 1, 15)

    time.sleep(0.1)
    current_ts = datetime.now(tz=timezone.utc)
    assert last_heartbeat_ts < current_ts

    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": creation_date,
        "last_change_ts": current_ts,
        "last_change_seqnum": 1,
        "principal": 1000,
        "interest": 12.0,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": current_ts - timedelta(days=1),
        "transfer_note_max_bytes": 500,
        "last_config_ts": last_ts,
        "last_config_seqnum": last_seqnum,
        "negligible_amount": negligible_amount,
        "config_flags": config_flags,
        "config_data": "",
        "account_id": str(C_ID),
        "debtor_info_iri": "http://example.com",
        "debtor_info_content_type": "text/plain",
        "debtor_info_sha256": 32 * b"\xff",
        "last_transfer_number": 22,
        "last_transfer_committed_at": current_ts - timedelta(days=2),
        "ts": current_ts,
        "ttl": 0,
    }

    p.process_account_update_signal(**params)
    ad = get_data()
    assert last_heartbeat_ts == ad.last_heartbeat_ts
    assert ad.last_change_seqnum == 0
    assert not ad.is_config_effectual
    assert ad.config_error is None

    params["ttl"] = 10000
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_heartbeat_ts > last_heartbeat_ts
    assert ad.is_config_effectual
    assert ad.creation_date == creation_date
    assert ad.last_change_ts == current_ts
    assert ad.last_change_seqnum == 1
    assert ad.principal == 1000
    assert ad.interest == 12.0
    assert ad.interest_rate == 5.0
    assert ad.last_interest_rate_change_ts == current_ts - timedelta(days=1)
    assert ad.transfer_note_max_bytes == 500
    assert ad.last_config_ts == last_ts
    assert ad.last_config_seqnum == last_seqnum
    assert ad.account_id == str(C_ID)
    assert ad.debtor_info_iri == "http://example.com"
    assert ad.debtor_info_content_type == "text/plain"
    assert ad.debtor_info_sha256 == 32 * b"\xff"
    assert ad.last_transfer_number == 22
    assert ad.last_transfer_committed_at == current_ts - timedelta(days=2)
    assert ad.config_error is None
    uls = models.UpdatedLedgerSignal.query.one()
    assert uls.creditor_id == C_ID
    assert uls.debtor_id == D_ID
    assert uls.update_id == ad.ledger_latest_update_id
    assert uls.account_id == str(C_ID)
    assert uls.creation_date == creation_date
    assert uls.principal == 0
    assert uls.last_transfer_number == 0
    assert uls.ts == ad.ledger_latest_update_ts

    p.process_account_update_signal(**params)
    assert ad.last_change_seqnum == 1

    p.process_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        config_ts=last_ts,
        config_seqnum=last_seqnum,
        negligible_amount=negligible_amount,
        config_data="",
        config_flags=config_flags,
        rejection_code="TEST_CONFIG_ERROR",
    )
    ad = get_data()
    assert ad.config_error == "TEST_CONFIG_ERROR"

    params["last_change_seqnum"] = 2
    params["principal"] = 1100
    params["negligible_amount"] = 3.33
    params["config_flags"] = 77
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 2
    assert ad.principal == 1100
    assert ad.negligible_amount == negligible_amount
    assert ad.config_flags == config_flags
    assert not ad.is_config_effectual
    assert ad.config_error == "TEST_CONFIG_ERROR"

    params["last_change_seqnum"] = 3
    params["negligible_amount"] = negligible_amount
    params["config_flags"] = config_flags
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 3
    assert ad.is_config_effectual
    assert ad.config_error is None

    params["creation_date"] = creation_date + timedelta(days=2)
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 3
    assert ad.is_config_effectual
    assert ad.creation_date == creation_date + timedelta(days=2)
    assert ad.config_error is None
    assert ad.ledger_principal == 0
    assert ad.ledger_last_entry_id == 89
    assert ad.ledger_last_transfer_number == 0
    assert len(models.UpdatedLedgerSignal.query.all()) == 2
    uls = models.UpdatedLedgerSignal.query.filter_by(
        creation_date=creation_date + timedelta(days=2)
    ).one()
    assert uls.creditor_id == C_ID
    assert uls.debtor_id == D_ID
    assert uls.update_id == ad.ledger_latest_update_id
    assert uls.account_id == str(C_ID)
    assert uls.principal == 0
    assert uls.last_transfer_number == 0
    assert uls.ts == ad.ledger_latest_update_ts

    # Discard orphaned account.
    params["debtor_id"] = 1235
    params["last_change_seqnum"] = 1
    params["negligible_amount"] = 2.0
    p.process_account_update_signal(**params)
    cas = ConfigureAccountSignal.query.filter_by(
        creditor_id=C_ID, debtor_id=1235
    ).one()
    assert cas.negligible_amount > 1e22
    assert cas.config_flags & AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    params["last_change_seqnum"] = 2
    params["negligible_amount"] = models.DEFAULT_NEGLIGIBLE_AMOUNT
    params["config_flags"] = AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    p.process_account_update_signal(**params)
    assert ConfigureAccountSignal.query.filter_by(
        creditor_id=C_ID, debtor_id=1235
    ).one()

    assert list(p.get_creditors_with_pending_log_entries()) == [(C_ID,)]
    p.process_pending_log_entries(1235)
    p.process_pending_log_entries(C_ID)
    assert (
        len(models.LogEntry.query.filter_by(object_type="AccountInfo").all())
        == 3
    )
    assert (
        len(
            models.LogEntry.query.filter_by(
                object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER
            ).all()
        )
        == 2
    )
    assert len(models.UpdatedLedgerSignal.query.all()) == 2


def test_process_rejected_config_signal(account):
    c = p.get_account_config(C_ID, D_ID)
    assert c.config_error is None
    p.process_pending_log_entries(C_ID)
    ple_count = len(models.LogEntry.query.all())

    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "config_ts": c.last_config_ts,
        "config_seqnum": c.last_config_seqnum,
        "negligible_amount": c.negligible_amount,
        "config_data": "",
        "config_flags": c.config_flags,
        "rejection_code": "TEST_CODE",
    }
    p.process_rejected_config_signal(**{**params, "config_data": "UNEXPECTED"})
    p.process_rejected_config_signal(
        **{**params, "negligible_amount": c.negligible_amount * 1.0001}
    )
    p.process_rejected_config_signal(
        **{**params, "config_flags": c.config_flags ^ 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_seqnum": c.last_config_seqnum - 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_seqnum": c.last_config_seqnum + 1}
    )
    p.process_rejected_config_signal(
        **{**params, "config_ts": c.last_config_ts + timedelta(seconds=-1)}
    )
    p.process_rejected_config_signal(
        **{**params, "config_ts": c.last_config_ts + timedelta(seconds=1)}
    )
    c = p.get_account_config(C_ID, D_ID)
    info_latest_update_id = c.info_latest_update_id
    info_latest_update_ts = c.info_latest_update_ts
    assert c.config_error is None
    assert len(models.PendingLogEntry.query.all()) == 0

    p.process_rejected_config_signal(**params)
    c = p.get_account_config(C_ID, D_ID)
    assert c.config_error == "TEST_CODE"
    assert c.info_latest_update_id == info_latest_update_id + 1
    assert c.info_latest_update_ts >= info_latest_update_ts

    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == ple_count + 1
    ple = LogEntry.query.filter_by(
        object_type="AccountInfo", object_update_id=info_latest_update_id + 1
    ).one()
    assert ple.creditor_id == C_ID
    assert "/info" in ple.object_uri

    p.process_rejected_config_signal(**params)
    assert len(LogEntry.query.all()) == ple_count + 1


def test_process_configure_account_signal(account, current_ts):
    p.process_configure_account_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        ts=current_ts,
        seqnum=123,
        negligible_amount=3.14,
        config_flags=3,
        config_data="test",
    )
    rcs = RejectedConfigSignal.query.one()
    assert rcs.debtor_id == D_ID
    assert rcs.creditor_id == C_ID
    assert rcs.config_ts == current_ts
    assert rcs.config_seqnum == 123
    assert rcs.config_flags == 3
    assert rcs.config_data == 'test'
    assert rcs.negligible_amount == 3.14
    assert rcs.rejection_code == 'NO_CONNECTION_TO_DEBTOR'


def test_process_account_purge_signal(account, current_ts):
    AccountData.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).update(
        {
            AccountData.creation_date: date(2020, 1, 2),
            AccountData.has_server_account: True,
            AccountData.principal: 1000,
            AccountData.interest: 15.0,
        },
        synchronize_session=False,
    )
    db.session.commit()
    data = AccountData.query.one()
    assert data.debtor_id == D_ID
    assert data.creditor_id == C_ID
    assert data.has_server_account
    assert data.principal == 1000
    assert data.interest == 15.0

    p.process_account_purge_signal(
        debtor_id=1111, creditor_id=2222, creation_date=date(2020, 1, 2)
    )
    p.process_account_purge_signal(
        debtor_id=D_ID, creditor_id=C_ID, creation_date=date(2020, 1, 1)
    )
    data = AccountData.query.one()
    assert data.has_server_account
    assert data.principal == 1000
    assert data.interest == 15.0
    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 2

    p.process_account_purge_signal(
        debtor_id=D_ID, creditor_id=C_ID, creation_date=date(2020, 1, 2)
    )
    data = AccountData.query.one()
    assert not data.has_server_account
    assert data.principal == 0
    assert data.interest == 0.0

    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 3
    entry = LogEntry.query.filter_by(object_type="AccountInfo").one()
    assert (
        entry.object_uri
        == f"/creditors/{i64_to_u64(C_ID)}/accounts/{i64_to_u64(D_ID)}/info"
    )
    assert not entry.is_deleted

    p.process_account_purge_signal(
        debtor_id=D_ID, creditor_id=C_ID, creation_date=date(2020, 1, 2)
    )
    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 3


def test_update_account_config(account, current_ts):
    def get_data():
        return AccountData.query.filter_by(
            creditor_id=C_ID, debtor_id=D_ID
        ).one()

    def get_info_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(LogEntry.query.filter_by(object_type="AccountInfo").all())

    creation_date = current_ts.date()
    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert data.config_data == ""
    assert data.config_flags == models.DEFAULT_CONFIG_FLAGS
    assert get_info_entries_count() == 0

    latest_update_id = p.get_account_config(C_ID, D_ID).config_latest_update_id
    p.update_account_config(
        C_ID,
        D_ID,
        is_scheduled_for_deletion=True,
        negligible_amount=1e30,
        allow_unsafe_deletion=False,
        config_data="TEST_CONFIG",
        latest_update_id=latest_update_id + 1,
    )

    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert data.config_data == "TEST_CONFIG"
    assert data.config_flags == (
        models.DEFAULT_CONFIG_FLAGS
        | AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    assert get_info_entries_count() == 0

    data = get_data()
    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": creation_date,
        "last_change_ts": current_ts,
        "last_change_seqnum": 1,
        "principal": 0,
        "interest": 0.0,
        "interest_rate": 0.0,
        "last_interest_rate_change_ts": models.TS0,
        "transfer_note_max_bytes": 500,
        "last_config_ts": data.last_config_ts,
        "last_config_seqnum": data.last_config_seqnum,
        "negligible_amount": data.negligible_amount,
        "config_flags": data.config_flags,
        "config_data": data.config_data,
        "account_id": str(C_ID),
        "debtor_info_iri": "http://example.com",
        "debtor_info_content_type": None,
        "debtor_info_sha256": None,
        "last_transfer_number": 0,
        "last_transfer_committed_at": models.TS0,
        "ts": current_ts,
        "ttl": 10000,
    }
    p.process_account_update_signal(**params)
    data = get_data()
    assert data.is_config_effectual
    assert not data.is_deletion_safe
    assert data.has_server_account
    assert data.config_data == "TEST_CONFIG"
    assert data.config_flags == (
        models.DEFAULT_CONFIG_FLAGS
        | AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    assert get_info_entries_count() == 1

    p.process_account_purge_signal(
        debtor_id=D_ID, creditor_id=C_ID, creation_date=creation_date
    )
    data = get_data()
    assert data.is_config_effectual
    assert data.is_deletion_safe
    assert not data.has_server_account
    assert data.config_data == "TEST_CONFIG"
    assert data.config_flags == (
        models.DEFAULT_CONFIG_FLAGS
        | AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    assert get_info_entries_count() == 2

    latest_update_id = p.get_account_config(C_ID, D_ID).config_latest_update_id
    p.update_account_config(
        C_ID,
        D_ID,
        is_scheduled_for_deletion=True,
        negligible_amount=1e30,
        allow_unsafe_deletion=False,
        config_data="",
        latest_update_id=latest_update_id + 1,
    )
    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert data.config_data == ""
    assert data.config_flags == (
        models.DEFAULT_CONFIG_FLAGS
        | AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    assert data.config_error is None
    assert get_info_entries_count() == 3


def test_process_account_transfer_signal(account, current_ts):
    def get_committed_tranfer_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(
            LogEntry.query.filter_by(
                object_type_hint=LogEntry.OTH_COMMITTED_TRANSFER
            ).all()
        )

    def has_pending_ledger_update():
        return (
            len(
                PendingLedgerUpdate.query.filter_by(
                    creditor_id=C_ID, debtor_id=D_ID
                ).all()
            )
            > 0
        )

    def delete_pending_ledger_update():
        PendingLedgerUpdate.query.filter_by(
            creditor_id=C_ID, debtor_id=D_ID
        ).delete()
        db.session.commit()

    assert not has_pending_ledger_update()
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2020, 1, 2),
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=12.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts - timedelta(days=1),
        transfer_note_max_bytes=500,
        last_config_ts=current_ts,
        last_config_seqnum=0,
        negligible_amount=100.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        config_data="",
        account_id=str(C_ID),
        debtor_info_iri="http://example.com",
        debtor_info_content_type=None,
        debtor_info_sha256=None,
        last_transfer_number=123,
        last_transfer_committed_at=current_ts - timedelta(days=2),
        ts=current_ts,
        ttl=10000,
    )
    assert has_pending_ledger_update()
    delete_pending_ledger_update()

    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": date(2020, 1, 2),
        "transfer_number": 1,
        "coordinator_type": "direct",
        "sender": "666",
        "recipient": str(C_ID),
        "acquired_amount": 100,
        "transfer_note_format": "json",
        "transfer_note": '{"message": "test"}',
        "committed_at": current_ts,
        "principal": 1000,
        "ts": current_ts - timedelta(days=6),
        "previous_transfer_number": 0,
        "retention_interval": timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 0
    assert get_committed_tranfer_entries_count() == 0
    assert not has_pending_ledger_update()

    params["retention_interval"] = timedelta(days=7)
    p.process_account_transfer_signal(**params)
    ct = CommittedTransfer.query.one()
    assert ct.debtor_id == D_ID
    assert ct.creditor_id == C_ID
    assert ct.creation_date == params["creation_date"]
    assert ct.transfer_number == 1
    assert ct.coordinator_type == "direct"
    assert ct.sender == "666"
    assert ct.recipient == str(C_ID)
    assert ct.acquired_amount == 100
    assert ct.transfer_note_format == params["transfer_note_format"]
    assert ct.transfer_note == params["transfer_note"]
    assert ct.committed_at == current_ts
    assert ct.principal == 1000
    assert ct.previous_transfer_number == 0
    assert get_committed_tranfer_entries_count() == 1
    assert has_pending_ledger_update()
    delete_pending_ledger_update()

    params["retention_interval"] = timedelta(days=7)
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 1
    assert get_committed_tranfer_entries_count() == 1
    le = LogEntry.query.filter_by(
        object_type_hint=LogEntry.OTH_COMMITTED_TRANSFER
    ).one()
    assert le.creditor_id == C_ID
    assert le.debtor_id == D_ID
    assert le.creation_date == params["creation_date"]
    assert le.transfer_number == 1
    assert not le.is_deleted
    assert le.object_update_id is None
    assert not has_pending_ledger_update()

    params["creditor_id"] = 1235
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 1
    assert get_committed_tranfer_entries_count() == 1


def test_get_pending_ledger_updates(db_session):
    assert p.get_pending_ledger_updates() == []
    assert p.get_pending_ledger_updates(max_count=10) == []


def test_process_pending_ledger_update(account, burst_count, current_ts):
    def get_ledger_update_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(
            LogEntry.query.filter_by(
                object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER
            ).all()
        )

    creation_date = date(2020, 1, 2)

    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": creation_date,
        "transfer_number": 1,
        "coordinator_type": "direct",
        "sender": "666",
        "recipient": str(C_ID),
        "acquired_amount": 1000,
        "transfer_note_format": "json",
        "transfer_note": '{"message": "test"}',
        "committed_at": current_ts,
        "principal": 1100,
        "ts": current_ts,
        "previous_transfer_number": 0,
        "retention_interval": timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)

    params["transfer_number"] = 20
    params["previous_transfer_number"] = 1
    params["principal"] = 2100
    p.process_account_transfer_signal(**params)

    params["transfer_number"] = 22
    params["previous_transfer_number"] = 21
    params["principal"] = 4150
    p.process_account_transfer_signal(**params)

    assert get_ledger_update_entries_count() == 0
    assert p.get_pending_ledger_updates() == []

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=creation_date,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=models.TS0,
        transfer_note_max_bytes=500,
        last_config_ts=current_ts,
        last_config_seqnum=0,
        negligible_amount=10.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        config_data="",
        account_id=str(C_ID),
        debtor_info_iri="http://example.com",
        debtor_info_content_type=None,
        debtor_info_sha256=None,
        last_transfer_number=0,
        last_transfer_committed_at=models.TS0,
        ts=current_ts,
        ttl=10000,
    )
    assert get_ledger_update_entries_count() == 1
    assert len(models.UpdatedLedgerSignal.query.all()) == 1
    assert p.get_pending_ledger_updates() == [(C_ID, D_ID)]
    assert (
        len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000))
        == 0
    )

    assert p.process_pending_ledger_update(
        2222, D_ID, burst_count=burst_count, max_delay=timedelta(days=10000)
    )
    assert p.process_pending_ledger_update(
        C_ID, 1111, burst_count=burst_count, max_delay=timedelta(days=10000)
    )
    assert get_ledger_update_entries_count() == 1
    assert len(models.UpdatedLedgerSignal.query.all()) == 1

    n = 0
    while not p.process_pending_ledger_update(
        C_ID, D_ID, burst_count=burst_count, max_delay=timedelta(days=10000)
    ):
        x = len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000))
        assert x > n
        n = x

    lue_count = get_ledger_update_entries_count()
    assert lue_count > 0
    assert (
        len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000))
        == 3
    )
    assert p.get_pending_ledger_updates() == []
    assert len(models.UpdatedLedgerSignal.query.all()) == lue_count

    params["transfer_number"] = 21
    params["previous_transfer_number"] = 20
    params["principal"] = 3150
    p.process_account_transfer_signal(**params)

    assert p.get_pending_ledger_updates() == [(C_ID, D_ID)]
    while not p.process_pending_ledger_update(
        C_ID, D_ID, burst_count=burst_count, max_delay=timedelta(days=10000)
    ):
        pass
    assert get_ledger_update_entries_count() > lue_count
    assert p.get_pending_ledger_updates() == []
    assert (
        len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000))
        == 6
    )

    log_entry = p.get_log_entries(C_ID, count=1000)[0][-1]
    assert log_entry.creditor_id == C_ID
    assert log_entry.object_type_hint == LogEntry.OTH_ACCOUNT_LEDGER
    assert log_entry.debtor_id == D_ID
    assert log_entry.object_update_id > 2
    assert not log_entry.is_deleted


def test_process_pending_ledger_update_missing_last_transfer(
    account, burst_count, current_ts
):
    def get_ledger_update_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(
            LogEntry.query.filter_by(
                object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER
            ).all()
        )

    creation_date = date(2020, 1, 2)
    assert get_ledger_update_entries_count() == 0
    ledger_latest_update_id = p.get_account_ledger(
        C_ID, D_ID
    ).ledger_latest_update_id

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=creation_date,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=models.TS0,
        transfer_note_max_bytes=500,
        last_config_ts=current_ts,
        last_config_seqnum=0,
        negligible_amount=10.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        config_data="",
        account_id=str(C_ID),
        debtor_info_iri=None,
        debtor_info_content_type=None,
        debtor_info_sha256=None,
        last_transfer_number=3,
        last_transfer_committed_at=current_ts - timedelta(days=20),
        ts=current_ts,
        ttl=10000,
    )

    assert len(PendingLedgerUpdate.query.all()) == 1
    max_delay = timedelta(days=30)
    while not p.process_pending_ledger_update(
        C_ID, D_ID, burst_count=burst_count, max_delay=max_delay
    ):
        pass
    assert len(PendingLedgerUpdate.query.all()) == 0
    lue_count = get_ledger_update_entries_count()
    assert lue_count == 1
    assert len(models.UpdatedLedgerSignal.query.all()) == lue_count
    data = p.get_account_ledger(C_ID, D_ID)
    ledger_last_entry_id = data.ledger_last_entry_id
    assert data.ledger_principal == 0
    assert data.ledger_last_entry_id >= 0
    assert data.ledger_last_transfer_number == 0
    assert data.ledger_latest_update_id == ledger_latest_update_id + 1
    assert len(PendingLedgerUpdate.query.all()) == 0

    max_delay = timedelta(days=10)
    p.ensure_pending_ledger_update(C_ID, D_ID)
    assert len(PendingLedgerUpdate.query.all()) == 1
    while not p.process_pending_ledger_update(
        C_ID, D_ID, burst_count=burst_count, max_delay=max_delay
    ):
        pass
    lue_count = get_ledger_update_entries_count()
    assert lue_count == 2
    assert len(models.UpdatedLedgerSignal.query.all()) == lue_count
    data = p.get_account_ledger(C_ID, D_ID)
    assert data.ledger_principal == 1000
    assert data.ledger_last_entry_id == ledger_last_entry_id + 1
    assert data.ledger_last_transfer_number == 3
    assert data.ledger_latest_update_id == ledger_latest_update_id + 2


def test_process_rejected_direct_transfer_signal(account, current_ts):
    rt = p.initiate_running_transfer(
        creditor_id=C_ID,
        transfer_uuid=TEST_UUID,
        debtor_id=D_ID,
        amount=1000,
        recipient_uri="swpt:18446744073709551615/666",
        recipient="666",
        transfer_note_format="json",
        transfer_note="{}",
        deadline=current_ts + timedelta(seconds=1000),
        final_interest_rate_ts=current_ts + timedelta(seconds=2000),
    )
    assert rt.creditor_id == C_ID
    assert rt.transfer_uuid == TEST_UUID
    assert rt.debtor_id == D_ID
    assert rt.amount == 1000
    assert rt.recipient_uri == "swpt:18446744073709551615/666"
    assert rt.recipient == "666"
    assert rt.transfer_note_format == "json"
    assert rt.transfer_note == "{}"
    assert isinstance(rt.initiated_at, datetime)
    assert rt.finalized_at is None
    assert rt.error_code is None
    assert rt.total_locked_amount is None
    assert rt.deadline == current_ts + timedelta(seconds=1000)
    assert rt.final_interest_rate_ts == current_ts + timedelta(seconds=2000)
    assert rt.coordinator_request_id is not None
    assert rt.transfer_id is None
    assert rt.latest_update_id == 1
    assert isinstance(rt.latest_update_ts, datetime)

    pts = PrepareTransferSignal.query.one()
    assert pts.creditor_id == C_ID
    assert pts.debtor_id == D_ID
    assert pts.coordinator_request_id == rt.coordinator_request_id
    assert pts.recipient == rt.recipient
    assert pts.final_interest_rate_ts == current_ts + timedelta(seconds=2000)
    assert 500 <= pts.max_commit_delay <= 1500

    p.process_rejected_direct_transfer_signal(
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        status_code="TEST_ERROR",
        total_locked_amount=600,
        debtor_id=D_ID,
        creditor_id=C_ID,
    )
    rt = RunningTransfer.query.one()
    assert rt.creditor_id == C_ID
    assert rt.transfer_uuid == TEST_UUID
    assert rt.debtor_id == D_ID
    assert rt.amount == 1000
    assert rt.recipient_uri == "swpt:18446744073709551615/666"
    assert rt.recipient == "666"
    assert rt.transfer_note_format == "json"
    assert rt.transfer_note == "{}"
    assert isinstance(rt.initiated_at, datetime)
    assert rt.finalized_at is not None
    assert rt.error_code == "TEST_ERROR"
    assert rt.total_locked_amount == 600
    assert rt.deadline == current_ts + timedelta(seconds=1000)
    assert rt.final_interest_rate_ts == current_ts + timedelta(seconds=2000)
    assert rt.coordinator_request_id is not None
    assert rt.transfer_id is None
    assert rt.latest_update_id == 2
    assert isinstance(rt.latest_update_ts, datetime)

    p.process_pending_log_entries(C_ID)
    le = (
        LogEntry.query.filter_by(object_type_hint=LogEntry.OTH_TRANSFER)
        .filter(LogEntry.object_update_id > 1)
        .one()
    )
    assert le.data is None
    assert le.data_finalized_at == rt.finalized_at
    assert le.data_error_code == rt.error_code


def test_process_rejected_direct_transfer_unexpected_error(
    account, current_ts
):
    rt = p.initiate_running_transfer(
        creditor_id=C_ID,
        transfer_uuid=TEST_UUID,
        debtor_id=D_ID,
        amount=1000,
        recipient_uri="swpt:18446744073709551615/666",
        recipient="666",
        transfer_note_format="json",
        transfer_note="{}",
        deadline=current_ts + timedelta(seconds=1000),
        final_interest_rate_ts=current_ts + timedelta(seconds=2000),
    )
    p.process_rejected_direct_transfer_signal(
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        status_code="TEST_ERROR",
        total_locked_amount=600,
        debtor_id=D_ID,
        creditor_id=666,
    )
    rt = RunningTransfer.query.one()
    assert rt.creditor_id == C_ID
    assert rt.transfer_uuid == TEST_UUID
    assert rt.debtor_id == D_ID
    assert rt.amount == 1000
    assert rt.finalized_at is not None
    assert rt.error_code == models.SC_UNEXPECTED_ERROR
    assert rt.total_locked_amount is None
    assert rt.transfer_id is None
    assert rt.latest_update_id == 2


def test_successful_transfer(account, current_ts):
    rt = p.initiate_running_transfer(
        creditor_id=C_ID,
        transfer_uuid=TEST_UUID,
        debtor_id=D_ID,
        amount=1000,
        recipient_uri="swpt:18446744073709551615/666",
        recipient="666",
        transfer_note_format="json",
        transfer_note="{}",
        deadline=current_ts + timedelta(seconds=1000),
        final_interest_rate_ts=current_ts + timedelta(seconds=2000),
    )
    p.process_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id + 1,
        locked_amount=0,
        recipient="666",
    )
    assert len(FinalizeTransferSignal.query.all()) == 1
    fts = FinalizeTransferSignal.query.filter_by(
        coordinator_request_id=rt.coordinator_request_id + 1
    ).one()
    assert fts.creditor_id == C_ID
    assert fts.debtor_id == D_ID
    assert fts.transfer_id == 123
    assert fts.coordinator_id == C_ID
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""
    p.process_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        locked_amount=0,
        recipient="666",
    )
    assert len(FinalizeTransferSignal.query.all()) == 2
    fts = FinalizeTransferSignal.query.filter_by(
        coordinator_request_id=rt.coordinator_request_id
    ).one()
    assert fts.creditor_id == C_ID
    assert fts.debtor_id == D_ID
    assert fts.transfer_id == 123
    assert fts.coordinator_id == C_ID
    assert fts.committed_amount == 1000
    assert fts.transfer_note_format == "json"
    assert fts.transfer_note == "{}"
    rt = RunningTransfer.query.one()
    assert rt.finalized_at is None
    assert rt.transfer_id == 123
    assert rt.error_code is None
    assert rt.total_locked_amount is None
    p.process_finalized_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        committed_amount=1000,
        status_code="OK",
        total_locked_amount=100,
    )
    assert rt.finalized_at is not None
    assert rt.transfer_id == 123
    assert rt.error_code is None
    assert rt.total_locked_amount is None

    p.process_pending_log_entries(C_ID)
    le = (
        LogEntry.query.filter_by(object_type_hint=LogEntry.OTH_TRANSFER)
        .filter(LogEntry.object_update_id > 1)
        .one()
    )
    assert le.data is None
    assert le.data_finalized_at == rt.finalized_at
    assert le.data_error_code is None


def test_unsuccessful_transfer(account, current_ts):
    rt = p.initiate_running_transfer(
        creditor_id=C_ID,
        transfer_uuid=TEST_UUID,
        debtor_id=D_ID,
        amount=1000,
        recipient_uri="swpt:18446744073709551615/666",
        recipient="666",
        transfer_note_format="json",
        transfer_note="{}",
        deadline=current_ts + timedelta(seconds=1000),
        final_interest_rate_ts=current_ts + timedelta(seconds=2000),
    )
    p.process_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        locked_amount=0,
        recipient="666",
    )
    with pytest.raises(p.ForbiddenTransferCancellation):
        p.cancel_running_transfer(C_ID, TEST_UUID)

    rt = RunningTransfer.query.one()
    assert rt.finalized_at is None
    assert rt.transfer_id == 123
    assert rt.error_code is None
    assert rt.total_locked_amount is None
    p.process_finalized_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        committed_amount=0,
        status_code="TEST_ERROR",
        total_locked_amount=100,
    )
    rt = RunningTransfer.query.one()
    assert rt.finalized_at is not None
    assert rt.transfer_id == 123
    assert rt.error_code == "TEST_ERROR"
    assert rt.total_locked_amount == 100


def test_unsuccessful_transfer_unexpected_error(account, current_ts):
    rt = p.initiate_running_transfer(
        creditor_id=C_ID,
        transfer_uuid=TEST_UUID,
        debtor_id=D_ID,
        amount=1000,
        recipient_uri="swpt:18446744073709551615/666",
        recipient="666",
        transfer_note_format="json",
        transfer_note="{}",
        deadline=current_ts + timedelta(seconds=1000),
        final_interest_rate_ts=current_ts + timedelta(seconds=2000),
    )
    p.process_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        locked_amount=0,
        recipient="666",
    )
    p.process_finalized_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_id=C_ID,
        coordinator_request_id=rt.coordinator_request_id,
        committed_amount=999,
        status_code="TEST_ERROR",
        total_locked_amount=100,
    )
    rt = RunningTransfer.query.one()
    assert rt.finalized_at is not None
    assert rt.transfer_id == 123
    assert rt.error_code == models.SC_UNEXPECTED_ERROR
    assert rt.total_locked_amount is None


def test_inspect_ops_procedures(creditor):
    assert p.is_account_creation_allowed(C_ID, 1, 1) is True
    assert p.is_account_creation_allowed(C_ID, 0, 1) is False
    assert p.is_account_creation_allowed(C_ID, 1, 0) is False

    assert p.is_account_reconfig_allowed(C_ID, 1) is True
    assert p.is_account_reconfig_allowed(C_ID, 0) is False

    assert p.is_transfer_creation_allowed(C_ID, 1, 1) is True
    assert p.is_transfer_creation_allowed(C_ID, 0, 1) is False
    assert p.is_transfer_creation_allowed(C_ID, 1, 0) is False

    p.register_account_creation(C_ID, 0)
    assert p.is_account_creation_allowed(C_ID, 2, 1) is True
    assert p.is_account_creation_allowed(C_ID, 1, 1) is False
    assert p.is_account_creation_allowed(C_ID, 2, 0) is False
    assert p.is_account_reconfig_allowed(C_ID, 1) is True
    assert p.is_account_reconfig_allowed(C_ID, 0) is False

    p.register_account_creation(C_ID, 10000)
    assert p.is_account_creation_allowed(C_ID, 3, 2) is True
    assert p.is_account_creation_allowed(C_ID, 2, 2) is False
    assert p.is_account_creation_allowed(C_ID, 3, 1) is False
    assert p.is_account_reconfig_allowed(C_ID, 2) is True
    assert p.is_account_reconfig_allowed(C_ID, 1) is False

    p.register_account_reconfig(C_ID, 10000)
    assert p.is_account_creation_allowed(C_ID, 3, 3) is True
    assert p.is_account_creation_allowed(C_ID, 2, 3) is False
    assert p.is_account_creation_allowed(C_ID, 3, 2) is False
    assert p.is_account_reconfig_allowed(C_ID, 3) is True
    assert p.is_account_reconfig_allowed(C_ID, 2) is False

    p.register_transfer_creation(C_ID, 0)
    assert p.is_transfer_creation_allowed(C_ID, 2, 1) is True
    assert p.is_transfer_creation_allowed(C_ID, 1, 1) is False
    assert p.is_transfer_creation_allowed(C_ID, 2, 0) is False

    p.register_transfer_creation(C_ID, 10000)
    assert p.is_transfer_creation_allowed(C_ID, 3, 2) is True
    assert p.is_transfer_creation_allowed(C_ID, 2, 2) is False
    assert p.is_transfer_creation_allowed(C_ID, 3, 1) is False

    assert p.is_account_creation_allowed(C_ID, 3, 3) is True
    p.increment_account_number(C_ID)
    assert p.is_account_creation_allowed(C_ID, 3, 3) is False
    assert p.is_account_creation_allowed(C_ID, 4, 3) is True
    p.decrement_account_number(C_ID)
    assert p.is_account_creation_allowed(C_ID, 3, 3) is True

    assert p.is_transfer_creation_allowed(C_ID, 3, 2) is True
    p.increment_transfer_number(C_ID)
    assert p.is_transfer_creation_allowed(C_ID, 3, 2) is False
    assert p.is_transfer_creation_allowed(C_ID, 4, 2) is True
    p.decrement_transfer_number(C_ID)
    assert p.is_transfer_creation_allowed(C_ID, 3, 2) is True
