from datetime import timedelta
from flask import redirect, url_for, request, current_app, g
from flask.views import MethodView
from flask_smorest import abort
from swpt_pythonlib.swpt_uris import parse_account_uri
from swpt_creditors.schemas import (
    examples,
    TransferCreationRequestSchema,
    TransferSchema,
    CommittedTransferSchema,
    TransferCancelationRequestSchema,
    ObjectReferencesPageSchema,
    TransfersPaginationParamsSchema,
)
from swpt_creditors import procedures
from swpt_creditors import inspect_ops
from .common import (
    context,
    parse_transfer_slug,
    ensure_creditor_permissions,
    Blueprint,
)
from .specs import DID, CID, TID, TRANSFER_UUID
from . import specs


transfers_api = Blueprint(
    "transfers",
    __name__,
    url_prefix="/creditors",
    description="""**Make transfers from one account to another account.**
    A new transfer record will be created for every initiated transfer. The
    client itself is responsible for the deletion of each transfer
    record, once the client does not need it anymore. Sometime after
    the transfer has been initiated, it will be automatically
    finalized as either successful or unsuccessful. Changes in the
    statuses of transfer records will be recorded to the corresponding
    creditor's "log". Note that the client may try to cancel an
    erroneously initiated transfer, but there are no guarantees for
    success.""",
)
transfers_api.before_request(ensure_creditor_permissions)


@transfers_api.route("/<i64:creditorId>/transfers/", parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(TransfersPaginationParamsSchema, location="query")
    @transfers_api.response(
        200,
        ObjectReferencesPageSchema(context=context),
        example=examples.TRANSFER_LINKS_EXAMPLE,
    )
    @transfers_api.doc(
        operationId="getTransfersPage", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, params, creditorId):
        """Return a collection of transfers, initiated by a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all transfers
        initiated by a given creditor, which have not been deleted
        yet. The returned fragment will not be sorted in any
        particular order.

        """

        n = current_app.config["APP_TRANSFERS_PER_PAGE"]
        transfer_uuids = procedures.get_creditor_transfer_uuids(
            creditorId, count=n, prev=params.get("prev")
        )
        items = [{"uri": str(uuid)} for uuid in transfer_uuids]

        if len(transfer_uuids) < n:
            # The last page does not have a 'next' link.
            return {
                "uri": request.full_path,
                "items": items,
            }

        return {
            "uri": request.full_path,
            "items": items,
            "next": f"?prev={transfer_uuids[-1]}",
        }

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(
        201, TransferSchema(context=context), headers=specs.LOCATION_HEADER
    )
    @transfers_api.doc(
        operationId="createTransfer",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={
            303: specs.TRANSFER_EXISTS,
            403: specs.FORBIDDEN_OPERATION,
            409: specs.TRANSFER_CONFLICT,
        },
    )
    def post(self, transfer_creation_request, creditorId):
        """Initiate a transfer.

        **Note:** This is a potentially dangerous operation which may
        require a PIN. Also, normally this is an idempotent operation,
        but when an incorrect PIN is supplied, repeating the operation
        may result in the creditor's PIN being blocked.

        """

        recipient_uri = transfer_creation_request["recipient_identity"]["uri"]
        try:
            debtorId, recipient = parse_account_uri(recipient_uri)
        except ValueError:
            abort(
                422,
                errors={
                    "json": {
                        "recipient": {
                            "uri": ["The URI can not be recognized."]
                        }
                    }
                },
            )

        uuid = transfer_creation_request["transfer_uuid"]
        location = url_for(
            "transfers.TransferEndpoint",
            _external=True,
            creditorId=creditorId,
            transferUuid=uuid,
        )
        try:
            inspect_ops.allow_transfer_creation(creditorId, debtorId)
            if not g.pin_reset_mode:
                procedures.verify_pin_value(
                    creditor_id=creditorId,
                    secret=current_app.config["PIN_PROTECTION_SECRET"],
                    pin_value=transfer_creation_request.get("optional_pin"),
                    pin_failures_reset_interval=timedelta(
                        days=current_app.config["APP_PIN_FAILURES_RESET_DAYS"]
                    ),
                )
            transfer = procedures.initiate_running_transfer(
                creditor_id=creditorId,
                transfer_uuid=uuid,
                debtor_id=debtorId,
                amount=transfer_creation_request["amount"],
                recipient_uri=recipient_uri,
                recipient=recipient,
                transfer_note_format=transfer_creation_request[
                    "transfer_note_format"
                ],
                transfer_note=transfer_creation_request["transfer_note"],
                final_interest_rate_ts=transfer_creation_request["options"][
                    "final_interest_rate_ts"
                ],
                deadline=transfer_creation_request["options"].get(
                    "optional_deadline"
                ),
                locked_amount=transfer_creation_request["options"][
                    "locked_amount"
                ],
            )
        except (inspect_ops.ForbiddenOperation, procedures.WrongPinValue):
            abort(403)
        except procedures.CreditorDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409)
        except procedures.TransferExists:
            return redirect(location, code=303)

        inspect_ops.register_transfer_creation(creditorId, debtorId)
        return transfer, {"Location": location}


@transfers_api.route(
    "/<i64:creditorId>/transfers/<uuid:transferUuid>",
    parameters=[CID, TRANSFER_UUID],
)
class TransferEndpoint(MethodView):
    @transfers_api.response(200, TransferSchema(context=context))
    @transfers_api.doc(
        operationId="getTransfer", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId, transferUuid):
        """Return a transfer."""

        return procedures.get_running_transfer(
            creditorId, transferUuid
        ) or abort(404)

    @transfers_api.arguments(TransferCancelationRequestSchema)
    @transfers_api.response(200, TransferSchema(context=context))
    @transfers_api.doc(
        operationId="cancelTransfer",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={403: specs.TRANSFER_CANCELLATION_FAILURE},
    )
    def post(self, cancel_transfer_request, creditorId, transferUuid):
        """Try to cancel a transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            transfer = procedures.cancel_running_transfer(
                creditorId, transferUuid
            )
        except procedures.ForbiddenTransferCancellation:  # pragma: no cover
            abort(403)
        except procedures.TransferDoesNotExist:
            abort(404)

        return transfer

    @transfers_api.response(204)
    @transfers_api.doc(
        operationId="deleteTransfer", security=specs.SCOPE_ACCESS_MODIFY
    )
    def delete(self, creditorId, transferUuid):
        """Delete a transfer.

        Before deleting a transfer, client implementations should
        ensure that at least 5 days (120 hours) have passed since the
        transfer was initiated (see the `initiatedAt` field). Also, it
        is recommended successful transfers to stay on the server at
        least a few weeks after their finalization.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        inspect_ops.decrement_transfer_number(creditorId, transferUuid)
        try:
            procedures.delete_running_transfer(creditorId, transferUuid)
        except procedures.TransferDoesNotExist:
            # NOTE: We decremented the transfer number before trying
            # to delete the running transfer, and now when we know
            # that the transfer did not exist, we increment the
            # transfer number again. This guarantees that in case of a
            # crash, the difference between the recorded number of
            # transfers and the real number of transfers will always
            # be in users' favor.
            inspect_ops.increment_transfer_number(creditorId, transferUuid)


@transfers_api.route(
    "/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<transferId>",
    parameters=[CID, DID, TID],
)
class CommittedTransferEndpoint(MethodView):
    @transfers_api.response(200, CommittedTransferSchema(context=context))
    @transfers_api.doc(
        operationId="getCommittedTransfer",
        security=specs.SCOPE_ACCESS_READONLY,
    )
    def get(self, creditorId, debtorId, transferId):
        """Return information about sent or received transfer.

        Note that the creditor can be either the sender of the
        recipient of the transfer.

        """

        try:
            creation_date, transfer_number = parse_transfer_slug(transferId)
        except ValueError:
            abort(404)

        return procedures.get_committed_transfer(
            creditorId, debtorId, creation_date, transfer_number
        ) or abort(404)
