#!/usr/bin/env python

try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv()

from swpt_creditors import create_app  # noqa
from swpt_creditors.extensions import protocol_broker  # noqa
import swpt_creditors.actors  # noqa

app = create_app()
protocol_broker.set_default()

if __name__ == '__main__':
    import sys
    print(
        "This script is intended to be imported by Dramatiq's CLI tools.\n"
        "\n"
        "Usage: dramatiq tasks:BROKER_NAME\n"
    )
    sys.exit(1)
