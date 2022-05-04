#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_python_http_api import SourcePythonHttpApi

if __name__ == "__main__":
    source = SourcePythonHttpApi()
    launch(source, sys.argv[1:])
