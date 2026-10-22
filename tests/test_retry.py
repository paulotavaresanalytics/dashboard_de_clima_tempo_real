
import contextlib
import importlib.util
import io
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, call
import requests

class RetryTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("clima_candidate", Path(__file__).resolve().parents[1] / "producer" / "producer.py")
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": "TEST_SECRET"}), patch("dotenv.load_dotenv"), patch("confluent_kafka.Producer"):
            spec.loader.exec_module(self.module)
        self.get = patch.object(self.module.requests, "get").start()
        self.sleep = patch.object(self.module.time, "sleep").start()
        patch.object(self.module.random, "uniform", return_value=0.25).start()
        self.addCleanup(patch.stopall)
        self.logs = io.StringIO()
        self.output = contextlib.redirect_stdout(self.logs)
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def response(self, status=200):
        result = Mock(status_code=status)
        result.json.return_value = {"main": {"temp": 25, "humidity": 70}, "weather": [{"description": "sol"}]}
        result.raise_for_status.side_effect = None if status == 200 else requests.HTTPError("TEST_SECRET", response=result)
        return result

    def test_success_without_retry(self):
        self.get.return_value = self.response()
        self.assertEqual(self.module.buscar_clima("Dubai")["temperatura"], 25)
        self.get.assert_called_once()
        self.assertEqual(self.get.call_args.kwargs["timeout"], 10)
        self.sleep.assert_not_called()

    def test_timeout_then_success(self):
        self.get.side_effect = [requests.Timeout("TEST_SECRET"), self.response()]
        self.assertIsNotNone(self.module.buscar_clima("Dubai"))
        self.sleep.assert_called_once_with(1.25)
        self.assertNotIn("TEST_SECRET", self.logs.getvalue())

    def test_connection_then_success(self):
        self.get.side_effect = [requests.ConnectionError(), self.response()]
        self.assertIsNotNone(self.module.buscar_clima("Dubai"))
        self.assertEqual(self.get.call_count, 2)

    def test_http_429_and_5xx_then_success(self):
        for status in (429, 500, 502, 503, 504, 599):
            with self.subTest(status=status):
                self.get.reset_mock()
                self.sleep.reset_mock()
                self.get.side_effect = [self.response(status), self.response()]
                self.assertIsNotNone(self.module.buscar_clima("Dubai"))
                self.assertEqual(self.get.call_count, 2)
                self.sleep.assert_called_once_with(1.25)

    def test_permanent_errors_not_retried(self):
        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status):
                self.get.reset_mock()
                self.sleep.reset_mock()
                self.get.side_effect = None
                self.get.return_value = self.response(status)
                self.assertIsNone(self.module.buscar_clima("Dubai"))
                self.get.assert_called_once()
                self.sleep.assert_not_called()
        self.assertNotIn("TEST_SECRET", self.logs.getvalue())

    def test_exhausted_attempts_exponential_wait(self):
        self.get.side_effect = requests.Timeout("TEST_SECRET")
        self.assertIsNone(self.module.buscar_clima("Dubai"))
        self.assertEqual(self.get.call_count, 3)
        self.assertEqual(self.sleep.call_args_list, [call(1.25), call(2.25)])
        self.assertNotIn("TEST_SECRET", self.logs.getvalue())

    def test_other_request_error_not_retried(self):
        self.get.side_effect = requests.RequestException("TEST_SECRET")
        self.assertIsNone(self.module.buscar_clima("Dubai"))
        self.get.assert_called_once()
        self.sleep.assert_not_called()
        self.assertNotIn("TEST_SECRET", self.logs.getvalue())

    def test_backoff_cap(self):
        self.module.BACKOFF_BASE = 100
        self.get.side_effect = [requests.Timeout(), self.response()]
        self.module.buscar_clima("Dubai")
        self.sleep.assert_called_once_with(10.0)

    def test_keyboard_interrupt_propagates(self):
        self.get.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.module.buscar_clima("Dubai")
        self.sleep.assert_not_called()

if __name__ == "__main__":
    unittest.main(verbosity=2)
