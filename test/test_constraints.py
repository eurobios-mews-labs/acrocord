# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import pytest

from acrocord.test import constraints


def test_example_span_table_constraint(
        get_example_dataframe_building):
    data = get_example_dataframe_building
    example_constraints = {
        "first_constraint": (constraints.unique, {"columns": ["building_id"]}),
        "second_constraint": (
            constraints.data_type,
            {'columns': ['building_id'], 'dtype': 'int64'}),
        "forth_constraint":
            (constraints.nb_unique_index,
             {"columns": ["building_id"], "minimum": 0,
              "maximum": 10}),
        "fifth_constraint": (constraints.quantile,
                             {"columns": ["building_id"], "q": 0.99,
                              "threshold": 1000}),
        "third_constraint": (
            constraints.not_nullable, {"columns": ["building_id"]})
    }

    dc = constraints.DataConstraints(data)
    dc.add_constraint(example_constraints)
    dc.test()


def test_example_span_table_constraint_raise(get_example_dataframe_building):
    data = get_example_dataframe_building
    example_constraints = {
        "first_constraint": (constraints.eligible_data_type, {}),
        "second_constraint": (
            constraints.data_type, {'columns': 'building_id'}),

    }

    dc = constraints.DataConstraints(data)
    dc.add_constraint(example_constraints)
    with pytest.raises(AssertionError):
        dc.test()
