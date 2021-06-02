from pathlib import Path
from unittest import mock

import os
import unittest

from drama.core.catalog.tests.HelloWorld import execute as exec_hello
from drama.core.catalog.tests.RandomNames import execute as exec_names
from drama.storage import LocalStorage

DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources')
NAMES = os.path.join(DIR, 'NAMES-F.TXT')


# Method for replacing requests.get
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def raise_for_status(self):
            pass

        @property
        def text(self):
            with open(NAMES, 'rt') as f:
                return f.read()

    return MockResponse()


class HelloWorldTestCase(unittest.TestCase):
    """
    Test case for the Hello World example.
    """
    def setUp(self) -> None:
        self.storage = LocalStorage(bucket_name="tests", folder_name=__name__)
        self.storage.setup()

    @mock.patch(
        'drama.core.catalog.tests.RandomNames.requests.get',
        side_effect=mocked_requests_get
    )
    def test_say_hello_to_everyone(self, mock_get):
        self.pcs = mock.MagicMock(storage=self.storage)
        # -- Generate names sample --------------------------------------------
        result = exec_names(
            self.pcs,
            sample_size=10,
            random_state=42
        )
        names_file = Path(result.files[0].resource)
        self.assertTrue(names_file.is_file())
        print('\nSelected names:')
        with names_file.open('rt') as f:
            print(''.join([line for line in f]))

        # -- Say Hey to everyone ----------------------------------------------
        self.pcs.get_from_upstream = mock.MagicMock(
            return_value={"TempFile": [{"resource": result.files[0].resource}]}
        )
        result = exec_hello(self.pcs, greeting='Hey', sleeptime=0.1)
        greetings_file = Path(result.files[0].resource)
        self.assertTrue(greetings_file.is_file())
        print('\nGreetings:')
        with greetings_file.open('rt') as f:
            print(''.join([line for line in f]))

    def tearDown(self) -> None:
        self.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
