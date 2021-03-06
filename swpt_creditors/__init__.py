__version__ = '0.1.0'

import os
import os.path
import logging
import logging.config
from flask_cors import CORS

# Configure app logging. If the value of "$APP_LOGGING_CONFIG_FILE" is
# a relative path, the directory of this (__init__.py) file will be
# used as a current directory.
config_filename = os.environ.get('APP_LOGGING_CONFIG_FILE')
if config_filename:  # pragma: no cover
    if not os.path.isabs(config_filename):
        current_dir = os.path.dirname(__file__)
        config_filename = os.path.join(current_dir, config_filename)
    logging.config.fileConfig(config_filename, disable_existing_loggers=False)
else:
    logging.basicConfig(level=logging.WARNING)


class MetaEnvReader(type):
    def __init__(cls, name, bases, dct):
        """MetaEnvReader class initializer.

        This function will get called when a new class which utilizes
        this metaclass is defined, as opposed to when an instance is
        initialized. This function overrides the default configuration
        from environment variables.

        """

        super().__init__(name, bases, dct)
        NoneType = type(None)
        annotations = dct.get('__annotations__', {})
        falsy_values = {'false', 'off', 'no', ''}
        for key, value in os.environ.items():
            if hasattr(cls, key):
                target_type = annotations.get(key) or type(getattr(cls, key))
                if target_type is NoneType:  # pragma: no cover
                    target_type = str

                if target_type is bool:
                    value = value.lower() not in falsy_values
                else:
                    value = target_type(value)

                setattr(cls, key, value)


class Configuration(metaclass=MetaEnvReader):
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE: int = None
    SQLALCHEMY_POOL_TIMEOUT: int = None
    SQLALCHEMY_POOL_RECYCLE: int = None
    SQLALCHEMY_MAX_OVERFLOW: int = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    PROTOCOL_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    API_TITLE = 'Creditors API'
    API_VERSION = 'v1'
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/creditors/.docs'
    OPENAPI_REDOC_PATH = ''
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = None  # or 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    APP_PROCESS_LOG_ADDITIONS_THREADS = 1
    APP_PROCESS_LOG_ADDITIONS_WAIT = 5.0
    APP_PROCESS_LOG_ADDITIONS_MAX_COUNT = 500000
    APP_PROCESS_LEDGER_UPDATES_THREADS = 1
    APP_PROCESS_LEDGER_UPDATES_BURST = 1000
    APP_PROCESS_LEDGER_UPDATES_MAX_COUNT = 500000
    APP_PROCESS_LEDGER_UPDATES_WAIT = 5.0
    APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT = 10000
    APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT = 10000
    APP_CREDITORS_SCAN_DAYS = 7.0
    APP_CREDITORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_CREDITORS_SCAN_BEAT_MILLISECS = 25
    APP_ACCOUNTS_SCAN_HOURS = 8.0
    APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 160
    APP_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_LOG_ENTRIES_SCAN_DAYS = 7.0
    APP_LOG_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LOG_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_LEDGER_ENTRIES_SCAN_DAYS = 7.0
    APP_LEDGER_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LEDGER_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_COMMITTED_TRANSFERS_SCAN_DAYS = 7.0
    APP_COMMITTED_TRANSFERS_SCAN_BLOCKS_PER_QUERY = 100
    APP_COMMITTED_TRANSFERS_SCAN_BEAT_MILLISECS = 35
    APP_TRANSFERS_FINALIZATION_AVG_SECONDS = 5.0
    APP_CREDITORS_PER_PAGE = 2000
    APP_LOG_ENTRIES_PER_PAGE = 100
    APP_ACCOUNTS_PER_PAGE = 100
    APP_TRANSFERS_PER_PAGE = 100
    APP_LEDGER_ENTRIES_PER_PAGE = 100
    APP_LOG_RETENTION_DAYS = 90.0
    APP_LEDGER_RETENTION_DAYS = 90.0
    APP_INACTIVE_CREDITOR_RETENTION_DAYS = 14.0
    APP_DEACTIVATED_CREDITOR_RETENTION_DAYS = 1826.0
    APP_MAX_HEARTBEAT_DELAY_DAYS = 365.0
    APP_MAX_TRANSFER_DELAY_DAYS = 14.0
    APP_MAX_CONFIG_DELAY_HOURS = 24.0
    APP_PIN_FAILURES_RESET_DAYS = 7.0
    APP_PIN_PROTECTION_SECRET = ''
    APP_SUPERUSER_SUBJECT_REGEX = '^creditors-superuser$'
    APP_SUPERVISOR_SUBJECT_REGEX = '^creditors-supervisor$'
    APP_CREDITOR_SUBJECT_REGEX = '^creditors:([0-9]+)$'


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_lib.utils import Int64Converter
    from .extensions import db, migrate, protocol_broker, api
    from .routes import admin_api, creditors_api, accounts_api, transfers_api, path_builder, specs
    from .schemas import type_registry
    from .cli import swpt_creditors
    from . import procedures
    from . import models  # noqa

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_port=1)
    app.url_map.converters['i64'] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config['API_SPEC_OPTIONS'] = specs.API_SPEC_OPTIONS
    CORS(app, max_age=24 * 60 * 60, vary_header=False, expose_headers=['Location'])
    db.init_app(app)
    migrate.init_app(app, db)
    protocol_broker.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    api.register_blueprint(creditors_api)
    api.register_blueprint(accounts_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_creditors)
    procedures.init(path_builder, type_registry)
    return app
