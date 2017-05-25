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
# limitations under the License.

import pickle
import logbook

log = logbook.Logger("State Persistence")

def persist_state(state_file_path, context, fields_prior):
    """
    Function to take the current state and persist it in Python pickle.
    Users can add anything they want to the state, provided the class can be
    be pickled.

    :param state_file_path: The path (including file name) of the stored state
    :param context: The algorithm's context
    :param fields_prior: fields used prior to initialize.  These will not be
    persisted.
    :return:
    """
    persistent_dict = {}
    fields_added_in_init = list(set(context.__dict__.keys()) - set(fields_prior))

    # pack the added fields into a dictionary to be written
    for field in fields_added_in_init:
        persistent_dict[field] = getattr(context, field)

    pickle.dump(persistent_dict, open(state_file_path, 'w'))


def unpersist_state(state_file_path, existing_state):
    """
    Loads a persisted state into the current algorithm's context.

    :param state_file_path: The path (including file name) of the stored state
    :param existing_state:
    :return:
    """
    saved_state = pickle.load(open(state_file_path, 'r'))

    for k,v in saved_state.items():
        setattr(existing_state, k, v)

    log.info("unpickled the state")
