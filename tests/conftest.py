import pytest
import sqlalchemy
import flask_migrate
from unittest import mock
from datetime import datetime, timezone
from swpt_creditors import create_app
from swpt_creditors.extensions import db

DB_SESSION = 'swpt_creditors.extensions.db.session'

config_dict = {
    'TESTING': True,
    'PREFERRED_URL_SCHEME': 'http',
    'APP_TRANSFERS_FINALIZATION_AVG_SECONDS': 10.0,
    'APP_MAX_TRANSFERS_PER_MONTH': 10,
    'APP_CREDITORS_PER_PAGE': 2,
    'APP_LOG_ENTRIES_PER_PAGE': 2,
    'APP_ACCOUNTS_PER_PAGE': 2,
    'APP_TRANSFERS_PER_PAGE': 2,
    'APP_LEDGER_ENTRIES_PER_PAGE': 2,
    'APP_LOG_RETENTION_DAYS': 31.0,
    'APP_LEDGER_RETENTION_DAYS': 31.0,
    'APP_MAX_TRANSFER_DELAY_DAYS': 14.0,
    'APP_INACTIVE_CREDITOR_RETENTION_DAYS': 14.0,
    'APP_DEACTIVATED_CREDITOR_RETENTION_DAYS': 1826.0,
    'APP_PIN_FAILURES_RESET_DAYS': 7.0,
    'APP_SUPERUSER_SUBJECT_REGEX': '^creditors-superuser$',
    'APP_SUPERVISOR_SUBJECT_REGEX': '^creditors-supervisor$',
    'APP_CREDITOR_SUBJECT_REGEX': '^creditors:([0-9]+)$',
}


def _restart_savepoint(session, transaction):
    if transaction.nested and not transaction._parent.nested:
        session.expire_all()
        session.begin_nested()


@pytest.fixture(scope='module')
def app_unsafe_session():
    app = create_app(config_dict)
    db.signalbus.autoflush = False
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope='module')
def app():
    """Create a Flask application object."""

    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        forbidden = mock.Mock()
        forbidden.side_effect = RuntimeError('Database accessed without "db_session" fixture.')
        with mock.patch(DB_SESSION, new=forbidden):
            yield app


@pytest.fixture(scope='function')
def db_session(app):
    """Create a mocked Flask-SQLAlchmey session object.

    The standard Flask-SQLAlchmey's session object is replaced with a
    mock session that perform all database operations in a
    transaction, which is rolled back at the end of the test.

    """

    db.signalbus.autoflush = False
    engines_by_table = db.get_binds()
    connections_by_engine = {engine: engine.connect() for engine in set(engines_by_table.values())}
    transactions = [connection.begin() for connection in connections_by_engine.values()]
    session_options = dict(
        binds={table: connections_by_engine[engine] for table, engine in engines_by_table.items()},
    )
    session = db.create_scoped_session(options=session_options)
    session.begin_nested()
    sqlalchemy.event.listen(session, 'after_transaction_end', _restart_savepoint)
    with mock.patch(DB_SESSION, new=session):
        yield session
    sqlalchemy.event.remove(session, 'after_transaction_end', _restart_savepoint)
    session.remove()
    for transaction in transactions:
        transaction.rollback()
    for connection in connections_by_engine.values():
        connection.close()


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)
