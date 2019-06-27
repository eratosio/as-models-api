# -*- coding: UTF-8 -*-
import datetime
import unittest

from as_models.util import sanitize_dict_for_json


class TestJsonUtils(unittest.TestCase):

    def test_sanitize_dict_valid_json(self):
        valid = {'name': 'a model descriptor',
                 'blob_ver': '1.2.3',
                 'data': [
                     1,
                     2,
                     3
                 ],
                 'an_object': {
                     'a_field': 1,
                     'pi_field': 3.14
                 },
                 'truthy': True,
                 'None': None
                 }
        self.assertDictEqual(valid, sanitize_dict_for_json(valid), msg='these are unequal')

    def test_sanitize_dict_invalid_json(self):
        dt_now = datetime.datetime.now()
        invalid = {'name': 'a model descriptor',
                   dt_now: '1.2.3',  # datetimes are not allows as keys. Expect it skipped
                   'data': [
                       1,
                       2,
                       3
                   ],
                   'an_object': {
                       'a_field': 1,
                       'pi_field': 3.14
                   },
                   'truthy': True,
                   'None': None
                   }
        round_tripped = sanitize_dict_for_json(invalid)
        # we anticipate that the round_tripped dict won't have the dodgy dt key/val pair.
        invalid.pop(dt_now)
        self.assertDictEqual(invalid, round_tripped, msg='these are unequal')

    def test_sanitize_datetimes_json(self):
        dt_now = datetime.datetime.now()
        invalid = {'now': dt_now,
                   'future': dt_now + datetime.timedelta(minutes=1),
                   'current_date': dt_now.date()}
        sanitized = sanitize_dict_for_json(invalid)
        # all values should now be strings, no datetimes.
        self.assertTrue(all([type(x) == str for x in sanitized.values()]))
