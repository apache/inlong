#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from pybind11.setup_helpers import Pybind11Extension, build_ext
from glob import glob
from setuptools import setup

import sys

__version__ = "0.0.1"

ext_modules = [
    Pybind11Extension("tubemq_client",
        sorted(glob("src/cpp/*.cc")),
        cxx_std=11,
        define_macros=[('VERSION_INFO', __version__)],
        )
]

setup(
    name="tubemq-client",
    version=__version__,
    author="dockerzhang",
    author_email="dockerzhang@apache.org",
    url="https://github.com/apache/incubator-tubemq/tree/tubemq-client-python",
    description="TubeMq Python SDK Client project built with pybind11",
    long_description="",
    ext_modules=ext_modules,
    extras_require={"test": "pytest"},
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)