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
import datetime
import os
import sys
import time
import warnings
from contextlib import contextmanager
from functools import wraps

from memory_profiler import memory_usage


class ColorsOut:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    Red = '\033[91m'
    Green = '\033[92m'
    Blue = '\033[94m'
    Cyan = '\033[96m'
    White = '\033[97m'
    Yellow = '\033[93m'
    Magenta = '\033[95m'
    Grey = '\033[90m'
    Black = '\033[90m'
    Default = '\033[99m'
    DEFAULT = Blue
    TIME = Grey


def print_(output, color="DEFAULT"):
    print(
        f"""{ColorsOut.__getattribute__(ColorsOut, color)}{output}{ColorsOut.ENDC}""")


def execution_time(method):
    @wraps(method)
    def timed(*args, **kw):
        if sys.platform =="win32":
            return method(*args, **kw)
        starting_time = time.time()
        mem, result = memory_usage((method, args, kw), retval=True, timeout=200,
                                   interval=1e-7)
        stopping_time = time.time()

        msg = "[" + method.__name__ + "] execution time :"
        msg += "-" * (40 - len(msg)) + "  "
        msg += str(datetime.timedelta(milliseconds=(stopping_time - starting_time) * 1000))
        msg += "  " + f'Memory {int(max(mem) - min(mem))}' + " MiB"
        if len(args) > 0 and hasattr(args[0], "active_execution_time"):
            if not args[0].active_execution_time:
                return result
            else:
                print_(msg, color="TIME")
        else:
            print_(msg, color="TIME")
        return result

    return timed


@contextmanager
def silence_stdout():
    new_target = open(os.devnull, "w")
    old_target = sys.stdout
    sys.stdout = new_target
    try:
        yield new_target
    finally:
        sys.stdout = old_target


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func
