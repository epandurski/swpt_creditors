from copy import copy
from marshmallow import (
    Schema,
    fields,
    validate,
    missing,
    pre_load,
    post_dump,
    pre_dump,
    validates,
    ValidationError,
)
from swpt_pythonlib.utils import i64_to_u64
from swpt_pythonlib.swpt_uris import make_account_uri
from swpt_creditors import models
from swpt_creditors.models import (
    T_INFINITY,
    MAX_INT64,
    CT_DIRECT,
    TRANSFER_NOTE_MAX_BYTES,
    TRANSFER_NOTE_FORMAT_REGEX,
    SC_INSUFFICIENT_AVAILABLE_AMOUNT,
)
from .common import (
    ObjectReferenceSchema,
    AccountIdentitySchema,
    ValidateTypeMixin,
    MutableResourceSchema,
    PinProtectedResourceSchema,
    type_registry,
    URI_DESCRIPTION,
    TYPE_DESCRIPTION,
)


_TRANSFER_NOTE_DESCRIPTION = (
    "A note from the sender. Can be any string that contains information which"
    " the sender wants the recipient to see, including an empty string."
)

_TRANSFER_NOTE_FORMAT_DESCRIPTION = (
    "The format used for the `note` field. An empty string signifies"
    " unstructured text."
)


def _make_invalid_account_uri(debtor_id: int) -> str:
    return f"swpt:{i64_to_u64(debtor_id)}/!"


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: type_registry.transfer_error,
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="TransferError",
        ),
    )
    error_code = fields.String(
        required=True,
        dump_only=True,
        data_key="errorCode",
        metadata=dict(
            description=(
                'The error code.\n\n* `"CANCELED_BY_THE_SENDER"` signifies'
                " that the transfer has been   canceled by the sender.\n*"
                ' `"SENDER_IS_UNREACHABLE"` signifies that the sender\'s'
                ' account does not exist, or can not make outgoing'
                ' transfers.\n* `"RECIPIENT_IS_UNREACHABLE"`'
                " signifies that the recipient's  account does not exist, or"
                " does not accept incoming transfers.\n*"
                ' `"TRANSFER_NOTE_IS_TOO_LONG"` signifies that the transfer'
                " has been   rejected because the byte-length of the transfer"
                ' note is too big.\n* `"INSUFFICIENT_AVAILABLE_AMOUNT"`'
                " signifies that the transfer   has been rejected due to"
                " insufficient amount available on the   sender's account.\n*"
                ' `"TIMEOUT"` signifies that the transfer has been terminated'
                " due to expired deadline.\n*"
                ' `"NEWER_INTEREST_RATE"` signifies that the transfer has'
                " been terminated because the current interest rate on the"
                " account is more recent than the specified final interest"
                " rate timestamp.\n"
            ),
            example="INSUFFICIENT_AVAILABLE_AMOUNT",
        ),
    )
    total_locked_amount = fields.Method(
        "get_total_locked_amount",
        data_key="totalLockedAmount",
        metadata=dict(
            type="integer",
            format="int64",
            description=(
                "This field will be present only when the transfer has been"
                " rejected due to insufficient available amount. In this case,"
                " it will contain the total sum secured (locked) for transfers"
                " on the account, *after* this transfer has been finalized."
            ),
            example=0,
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "errorCode" in obj
        return obj

    def get_total_locked_amount(self, obj):
        if obj["error_code"] != SC_INSUFFICIENT_AVAILABLE_AMOUNT:
            return missing
        return obj.get("total_locked_amount") or 0


class TransferOptionsSchema(Schema):
    type = fields.String(
        load_default=type_registry.transfer_options,
        dump_default=type_registry.transfer_options,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="TransferOptions",
        ),
    )
    final_interest_rate_ts = fields.DateTime(
        load_default=T_INFINITY,
        data_key="finalInterestRateTimestamp",
        metadata=dict(
            description=(
                "When the transferred amount would need to be changed if the"
                " interest rate on the account had been changed unexpectedly"
                " by the server, this field specifies the onset moment of the"
                " account's current interest rate, from the client's"
                " perspective. The default value is appropriate for normal"
                " transfers."
            ),
            example="9999-12-31T23:59:59+00:00",
        ),
    )
    optional_deadline = fields.DateTime(
        data_key="deadline",
        metadata=dict(
            description=(
                "The transfer will be successful only if it is committed"
                " before this moment. This can be useful, for example, when"
                " the transferred amount may need to be changed if the"
                " transfer can not be committed in time. When this field is"
                " not present, this means that the deadline for the transfer"
                " will not be earlier than normal."
            ),
        ),
    )
    locked_amount = fields.Integer(
        load_default=0,
        validate=validate.Range(min=0, max=MAX_INT64),
        data_key="lockedAmount",
        metadata=dict(
            format="int64",
            description=(
                "The amount that should to be locked when the transer is"
                " prepared. This must be a non-negative number."
            ),
            example=0,
        ),
    )


class TransferResultSchema(Schema):
    type = fields.Function(
        lambda obj: type_registry.transfer_result,
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="TransferResult",
        ),
    )
    finalized_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="finalizedAt",
        metadata=dict(
            description="The moment at which the transfer was finalized.",
        ),
    )
    committed_amount = fields.Integer(
        required=True,
        dump_only=True,
        data_key="committedAmount",
        metadata=dict(
            format="int64",
            description=(
                "The transferred amount. If the transfer has been successful,"
                " the value will be equal to the requested transfer amount"
                " (always a positive number). If the transfer has been"
                " unsuccessful, the value will be zero."
            ),
            example=0,
        ),
    )
    error = fields.Nested(
        TransferErrorSchema,
        dump_only=True,
        metadata=dict(
            description=(
                "An error that has occurred during the execution of the"
                " transfer. This field will be present if, and only if, the"
                " transfer has been unsuccessful."
            ),
        ),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert "finalizedAt" in obj
        assert "committedAmount" in obj
        return obj


class TransferCreationRequestSchema(
    ValidateTypeMixin, PinProtectedResourceSchema
):
    type = fields.String(
        load_default=type_registry.transfer_creation_request,
        dump_default=type_registry.transfer_creation_request,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="TransferCreationRequest",
        ),
    )
    transfer_uuid = fields.UUID(
        required=True,
        data_key="transferUuid",
        metadata=dict(
            description="A client-generated UUID for the transfer.",
            example="123e4567-e89b-12d3-a456-426655440000",
        ),
    )
    recipient_identity = fields.Nested(
        AccountIdentitySchema,
        required=True,
        data_key="recipient",
        metadata=dict(
            description="The recipient's `AccountIdentity` information.",
            example={"type": "AccountIdentity", "uri": "swpt:1/2222"},
        ),
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        metadata=dict(
            format="int64",
            description=(
                "The amount that has to be transferred. Must be a non-negative"
                " number. Setting this value to zero can be useful when the"
                " sender wants to verify whether the recipient's account"
                " exists and accepts incoming transfers."
            ),
            example=1000,
        ),
    )
    transfer_note_format = fields.String(
        load_default="",
        validate=validate.Regexp(TRANSFER_NOTE_FORMAT_REGEX),
        data_key="noteFormat",
        metadata=dict(
            description=_TRANSFER_NOTE_FORMAT_DESCRIPTION,
            example="",
        ),
    )
    transfer_note = fields.String(
        load_default="",
        validate=validate.Length(max=TRANSFER_NOTE_MAX_BYTES),
        data_key="note",
        metadata=dict(
            description=_TRANSFER_NOTE_DESCRIPTION,
            example="Hello, World!",
        ),
    )
    options = fields.Nested(
        TransferOptionsSchema,
        metadata=dict(
            description="Optional `TransferOptions`.",
        ),
    )

    @pre_load
    def ensure_options(self, data, many, partial):
        if "options" not in data:
            data = data.copy()
            data["options"] = {}
        return data

    @validates("transfer_note")
    def validate_transfer_note(self, value):
        if len(value.encode("utf8")) > TRANSFER_NOTE_MAX_BYTES:
            raise ValidationError(
                "The total byte-length of the note exceeds"
                f" {TRANSFER_NOTE_MAX_BYTES} bytes."
            )


class TransferSchema(TransferCreationRequestSchema, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example=(
                "/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000"
            ),
        ),
    )
    type = fields.Function(
        lambda obj: type_registry.transfer,
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="Transfer",
        ),
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key="transfersList",
        metadata=dict(
            description="The URI of creditor's `TransfersList`.",
            example={"uri": "/creditors/2/transfers-list"},
        ),
    )
    transfer_note_format = fields.String(
        required=True,
        dump_only=True,
        data_key="noteFormat",
        metadata=dict(
            pattern=TRANSFER_NOTE_FORMAT_REGEX,
            description=_TRANSFER_NOTE_FORMAT_DESCRIPTION,
            example="",
        ),
    )
    transfer_note = fields.String(
        required=True,
        dump_only=True,
        data_key="note",
        metadata=dict(
            description=_TRANSFER_NOTE_DESCRIPTION,
            example="Hello, World!",
        ),
    )
    options = fields.Nested(
        TransferOptionsSchema,
        required=True,
        dump_only=True,
        metadata=dict(
            description="Transfer's `TransferOptions`.",
        ),
    )
    initiated_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="initiatedAt",
        metadata=dict(
            description="The moment at which the transfer was initiated.",
        ),
    )
    checkup_at = fields.Method(
        "get_checkup_at_string",
        data_key="checkupAt",
        metadata=dict(
            type="string",
            format="date-time",
            description=(
                "The moment at which the sender is advised to look at the"
                " transfer again, to see if it's status has changed. If this"
                " field is not present, this means either that the status of"
                " the transfer is not expected to change, or that the moment"
                " of the expected change can not be predicted.\n\n**Note:**"
                " The value of this field is calculated on-the-fly, so it may"
                " change from one request to another, and no `LogEntry` for"
                " the change will be added to the log."
            ),
        ),
    )
    result = fields.Nested(
        TransferResultSchema,
        dump_only=True,
        metadata=dict(
            description=(
                "Contains information about the outcome of the transfer. This"
                " field will be preset if, and only if, the transfer has been"
                " finalized. Note that a finalized transfer can be either"
                " successful, or unsuccessful."
            ),
        ),
    )

    @pre_dump
    def process_running_transfer_instance(self, obj, many):
        assert isinstance(obj, models.RunningTransfer)
        paths = self.context["paths"]
        obj = copy(obj)
        obj.uri = paths.transfer(
            creditorId=obj.creditor_id, transferUuid=obj.transfer_uuid
        )
        obj.transfers_list = {
            "uri": paths.transfers_list(creditorId=obj.creditor_id)
        }
        obj.recipient_identity = {"uri": obj.recipient_uri}
        obj.options = {
            "final_interest_rate_ts": obj.final_interest_rate_ts,
            "locked_amount": obj.locked_amount,
        }

        if obj.deadline is not None:
            obj.options["optional_deadline"] = obj.deadline

        if obj.finalized_at:
            result = {"finalized_at": obj.finalized_at}

            error_code = obj.error_code
            if error_code is None:
                result["committed_amount"] = obj.amount
            else:
                result["committed_amount"] = 0
                result["error"] = {
                    "error_code": error_code,
                    "total_locked_amount": obj.total_locked_amount,
                }

            obj.result = result

        return obj

    def get_checkup_at_string(self, obj):
        if obj.finalized_at:
            return missing

        calc_checkup_datetime = self.context["calc_checkup_datetime"]
        return calc_checkup_datetime(
            obj.debtor_id, obj.initiated_at
        ).isoformat()


class TransferCancelationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default=type_registry.transfer_cancelation_request,
        dump_default=type_registry.transfer_cancelation_request,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="TransferCancelationRequest",
        ),
    )


class CommittedTransferSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        metadata=dict(
            format="uri-reference",
            description=URI_DESCRIPTION,
            example="/creditors/2/accounts/1/transfers/18444-999",
        ),
    )
    type = fields.Function(
        lambda obj: type_registry.committed_transfer,
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="CommittedTransfer",
        ),
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        metadata=dict(
            description="The URI of the affected `Account`.",
            example={"uri": "/creditors/2/accounts/1/"},
        ),
    )
    rationale = fields.String(
        dump_only=True,
        metadata=dict(
            description=(
                "This field will be present only for system transfers. Its"
                " value indicates the subsystem which originated the transfer."
                ' For interest payments the value will be `"interest"`. For'
                " transfers that create new money into existence, the value"
                ' will be `"issuing"`.'
            ),
            example="interest",
        ),
    )
    sender_identity = fields.Nested(
        AccountIdentitySchema,
        required=True,
        dump_only=True,
        data_key="sender",
        metadata=dict(
            description="The sender's `AccountIdentity` information.",
            example={"type": "AccountIdentity", "uri": "swpt:1/2"},
        ),
    )
    recipient_identity = fields.Nested(
        AccountIdentitySchema,
        required=True,
        dump_only=True,
        data_key="recipient",
        metadata=dict(
            description="The recipient's `AccountIdentity` information.",
            example={"type": "AccountIdentity", "uri": "swpt:1/2222"},
        ),
    )
    acquired_amount = fields.Integer(
        required=True,
        dump_only=True,
        data_key="acquiredAmount",
        metadata=dict(
            format="int64",
            description=(
                "The amount that this transfer has added to the account's"
                " principal. This can be a positive number (an incoming"
                " transfer), a negative number (an outgoing transfer), but can"
                " not be zero."
            ),
            example=1000,
        ),
    )
    transfer_note_format = fields.String(
        required=True,
        dump_only=True,
        data_key="noteFormat",
        metadata=dict(
            pattern=TRANSFER_NOTE_FORMAT_REGEX,
            description=_TRANSFER_NOTE_FORMAT_DESCRIPTION,
            example="",
        ),
    )
    transfer_note = fields.String(
        required=True,
        dump_only=True,
        data_key="note",
        metadata=dict(
            description=(
                "A note from the committer of the transfer. Can be any string"
                " that contains information which whoever committed the"
                " transfer wants the recipient (and the sender) to see. Can be"
                " an empty string."
            ),
            example="",
        ),
    )
    committed_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="committedAt",
        metadata=dict(
            description="The moment at which the transfer was committed.",
        ),
    )

    @pre_dump
    def process_committed_transfer_instance(self, obj, many):
        assert isinstance(obj, models.CommittedTransfer)
        paths = self.context["paths"]
        obj = copy(obj)
        obj.uri = paths.committed_transfer(
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
            creationDate=obj.creation_date,
            transferNumber=obj.transfer_number,
        )
        obj.account = {
            "uri": paths.account(
                creditorId=obj.creditor_id, debtorId=obj.debtor_id
            )
        }

        try:
            sender_uri = make_account_uri(obj.debtor_id, obj.sender)
        except ValueError:
            sender_uri = _make_invalid_account_uri(obj.debtor_id)
        obj.sender_identity = {"uri": sender_uri}

        try:
            recipient_uri = make_account_uri(obj.debtor_id, obj.recipient)
        except ValueError:
            recipient_uri = _make_invalid_account_uri(obj.debtor_id)
        obj.recipient_identity = {"uri": recipient_uri}

        coordinator_type = obj.coordinator_type
        if coordinator_type != CT_DIRECT:
            obj.rationale = coordinator_type

        return obj


class TransfersPaginationParamsSchema(Schema):
    prev = fields.UUID(
        load_only=True,
        metadata=dict(
            description=(
                "The returned fragment will begin with the first transfer that"
                " follows the transfer whose transfer UUID is equal to value"
                " of this parameter."
            ),
            example="123e4567-e89b-12d3-a456-426655440000",
        ),
    )
