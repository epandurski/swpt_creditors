#!/bin/sh
set -e

# During development, we should be able to connect to services
# installed on "localhost" from the container. To allow this, we find
# the IP address of the docker host, and then for each variable name
# $SUBSTITUTE_LOCALHOST_IN_VARS, we substitute "localhost" with that
# IP address.
host_ip=$(ip route show | awk '/default/ {print $3}')
for envvar_name in $SUBSTITUTE_LOCALHOST_IN_VARS; do
    eval envvar_value=\$$envvar_name
    if [[ -n $envvar_value ]]; then
        eval export $envvar_name=$(echo "$envvar_value" | sed -E "s/(.*@|.*\/\/)localhost\b/\1$host_ip/")
    fi
done

# This function tries to upgrade the database schema with exponential
# backoff. This is necessary during development, because the database
# might not be running yet when this script executes.
perform_db_upgrade() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-db-upgrade.error"
    echo -n 'Running database schema upgrade ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask db upgrade 2>$error_file; then
            perform_db_initialization
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

setup_rabbitmq_bindings() {
    flask swpt_creditors subscribe swpt_creditors
    return 0
}

# This function is intended to perform additional one-time database
# initialization. Make sure that it is idempotent.
# (https://en.wikipedia.org/wiki/Idempotence)
perform_db_initialization() {
    flask swpt_creditors configure_interval -- $MIN_CREDITOR_ID $MAX_CREDITOR_ID
}

configure_web_server() {
    export GUNICORN_LOGLEVEL=${WEBSERVER_LOGLEVEL:-warning}
    export GUNICORN_WORKERS=${WEBSERVER_WORKERS:-1}
    export GUNICORN_THREADS=${WEBSERVER_THREADS:-3}
    envsubst '$PORT $OAUTH2_INTROSPECT_URL' \
             < "$APP_ROOT_DIR/oathkeeper/config.yaml.template" \
             > "$APP_ROOT_DIR/oathkeeper/config.yaml"
    envsubst '$RESOURCE_SERVER' \
             < "$APP_ROOT_DIR/oathkeeper/rules.json.template" \
             > "$APP_ROOT_DIR/oathkeeper/rules.json"
}

case $1 in
    develop-run-flask)
        shift
        exec flask run --host=0.0.0.0 --port $PORT --without-threads "$@"
        ;;
    test)
        perform_db_upgrade
        exec pytest
        ;;
    configure)
        perform_db_upgrade
        setup_rabbitmq_bindings
        ;;
    webserver)
        configure_web_server
        exec supervisord -c "$APP_ROOT_DIR/supervisord-webserver.conf"
        ;;
    protocol)
        exec dramatiq --processes ${PROTOCOL_PROCESSES-1} --threads ${PROTOCOL_THREADS-3} tasks:protocol_broker
        ;;
    process_ledger_updates | process_log_additions | scan_creditors | scan_accounts | scan_committed_transfers \
        | scan_ledger_entries | scan_log_entries | configure_interval)
        exec flask swpt_creditors "$@"
        ;;
    flush_configure_accounts  | flush_prepare_transfers | flush_finalize_transfers)
        flush_configure_accounts=ConfigureAccountSignal
        flush_prepare_transfers=PrepareTransferSignal
        flush_finalize_transfers=FinalizeTransferSignal

        # For example: if `$1` is "flush_configure_accounts",
        # `signal_name` will be "ConfigureAccountSignal".
        eval signal_name=\$$1

        # For example: if `$1` is "flush_configure_accounts", `wait`
        # will get the value of the APP_FLUSH_CONFIGURE_ACCOUNTS_WAIT
        # environment variable, defaulting to 5 if it is not defined.
        eval wait=\${APP_$(echo "$1" | tr [:lower:] [:upper:])_WAIT-5}

        exec flask signalbus flushmany --repeat=$wait $signal_name
        ;;
    all)
        configure_web_server
        exec supervisord -c "$APP_ROOT_DIR/supervisord-all.conf"
        ;;
    *)
        exec "$@"
        ;;
esac
