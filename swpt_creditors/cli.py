import logging
import click
from os import environ
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from . import procedures
from .extensions import db


@click.group('swpt_creditors')
def swpt_creditors():
    """Perform swpt_creditors specific operations."""


@swpt_creditors.command()
@with_appcontext
@click.argument('queue_name')
def subscribe(queue_name):  # pragma: no cover
    """Subscribe a queue for the observed events and messages.

    QUEUE_NAME specifies the name of the queue.

    """

    from .extensions import broker, MAIN_EXCHANGE_NAME
    from . import actors  # noqa

    channel = broker.channel
    channel.exchange_declare(MAIN_EXCHANGE_NAME)
    click.echo(f'Declared "{MAIN_EXCHANGE_NAME}" direct exchange.')

    if environ.get('APP_USE_LOAD_BALANCING_EXCHANGE', '') not in ['', 'False']:
        bind = channel.exchange_bind
        unbind = channel.exchange_unbind
    else:
        bind = channel.queue_bind
        unbind = channel.queue_unbind
    bind(queue_name, MAIN_EXCHANGE_NAME, queue_name)
    click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{queue_name}".')

    for actor in [broker.get_actor(actor_name) for actor_name in broker.get_declared_actors()]:
        if 'event_subscription' in actor.options:
            routing_key = f'events.{actor.actor_name}'
            if actor.options['event_subscription']:
                bind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{routing_key}".')
            else:
                unbind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Unsubscribed "{queue_name}" from "{MAIN_EXCHANGE_NAME}.{routing_key}".')


@swpt_creditors.command('process_log_entries')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
def process_log_entries(threads):
    """Process all pending log entries."""

    # TODO: Python with SQLAlchemy can process about 1000 accounts per
    # second. (It is CPU bound!) This might be insufficient if we have
    # a highly perfomant database server. In this case we should
    # either distribute the processing to several machines, or improve
    # on python's code performance.

    threads = threads or int(environ.get('APP_PROCESS_LOG_ENTRIES_THREADS', '1'))
    app = current_app._get_current_object()

    def push_app_context():
        ctx = app.app_context()
        ctx.push()

    def log_error(e):  # pragma: no cover
        try:
            raise e
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Caught error while processing log entries.')

    pool = ThreadPool(threads, initializer=push_app_context)
    for creditor_id in procedures.get_creditors_with_pending_log_entries():
        pool.apply_async(procedures.process_pending_log_entries, (creditor_id,), error_callback=log_error)
    pool.close()
    pool.join()
