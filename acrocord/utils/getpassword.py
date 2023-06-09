# Copyright 2023 Eurobios
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#
import getpass
import sys
import tempfile


def pass_(path="."):
    """

    Parameters
    ----------
    path

    Returns
    -------

    """
    password = getpass.getpass(prompt="Enter the Password:")
    tmp = tempfile.NamedTemporaryFile(dir=path, delete=False)
    tmp.write(bytearray(password, encoding="utf-8"))


def pass_win():
    return getpass.win_getpass(prompt="Enter the Password:", stream=sys.stdout)


if __name__ == "__main__":
    pass_(sys.argv[1])
