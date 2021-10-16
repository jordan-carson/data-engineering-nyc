import sys
sys.path.append(r'/Users/jordancarson/Projects/JPM/data-engineering-nyc')

import unittest
import requests_mock
from unittest.mock import patch
from src.api.get_data import get_response


class BasicTests(unittest.TestCase):
    @patch('requests.get') # decorator to mock the requests.get method
    def test_request_response(self, mock_get):
        mock_get.return_value.status_code = 200
        res = get_response()

        self.assertEqual(res.status_code, 200)


if __name__ == '__main__':
    unittest.main()