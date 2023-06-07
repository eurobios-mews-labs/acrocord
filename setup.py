# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

from setuptools import setup, find_packages


def parse_requirements(filename):
    """ load requirements from a pip requirements file """
    line_iter = (line.strip() for line in open(filename))
    return [line for line in line_iter if line and not line.startswith("#")]


install_req = parse_requirements("requirements.txt")

setup(name='acrocord',
      version='2023.2.1',
      description='Python API for PostreSQL database',
      url='https://github.com/eurobios-scb/acrocord',
      author='Eurobios',
      author_email='contact_ost@eurobios.com',
      license='Apache-2.0',
      install_requires=install_req,
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False)
