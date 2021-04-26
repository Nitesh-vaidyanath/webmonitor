import unittest
import web_monitor
import unittest
from unittest.mock import patch

class WebMonitorTest(unittest.TestCase):
    def setUp(self):
        self.obj = web_monitor.WebSiteMonitor("", "")
        self.obj1 = web_monitor.WebSiteMonitor("", "mock")

    def test_httpget(self):
        with patch("web_monitor.requests.get") as mocked_get:
            mocked_get.return_value.status_code = 404
            mocked_get.return_value.text = "This is the mock test"
            request = self.obj.httpGet(url='http://test.com')
            mocked_get.assert_called_with('http://test.com')
            self.assertEqual(request["response_code"], 404)
            self.assertEqual(request["regex_matched"], "")

    def test_httpget2(self):
        with patch("web_monitor.requests.get") as mocked_get:
            mocked_get.return_value.status_code = 200
            mocked_get.return_value.text = "This is the mock test"
            request = self.obj1.httpGet(url='http://test.com')
            mocked_get.assert_called_with('http://test.com')
            self.assertEqual(request["response_code"], 200)
            self.assertEqual(request["regex_matched"], "mock")

if __name__ == "__main__":
   unittest.main()
