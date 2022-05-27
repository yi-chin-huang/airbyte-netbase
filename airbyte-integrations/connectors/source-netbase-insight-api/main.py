#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_netbase_insight_api import SourceNetbaseInsightApi

if __name__ == "__main__":
    source = SourceNetbaseInsightApi()
    launch(source, sys.argv[1:])
