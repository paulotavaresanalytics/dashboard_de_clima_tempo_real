import importlib.util
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "import_results.py"
SPEC = importlib.util.spec_from_file_location("import_results", MODULE_PATH)
import_results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(import_results)


class ImportResultsTests(unittest.TestCase):
    def test_parse_unix_timestamp(self):
        parsed = import_results.parse_timestamp("1793011200")
        self.assertEqual(parsed, datetime.fromtimestamp(1793011200, tz=timezone.utc))

    def test_parse_iso_timestamp_normalizes_timezone(self):
        parsed = import_results.parse_timestamp("2026-10-26T12:00:00-03:00")
        self.assertEqual(parsed, datetime(2026, 10, 26, 15, 0, tzinfo=timezone.utc))

    def test_parse_evento(self):
        event = import_results.parse_evento({
            "cidade": " São Paulo ",
            "temperatura": "24.5",
            "umidade": "70",
            "descricao": "céu limpo",
            "timestamp": "1793011200",
        })
        self.assertEqual(event[0:4], ("São Paulo", 24.5, 70, "céu limpo"))
        self.assertEqual(event[4], datetime.fromtimestamp(1793011200, tz=timezone.utc))

    def test_ler_eventos_rejects_invalid_line(self):
        with tempfile.TemporaryDirectory() as diretorio:
            path = Path(diretorio) / "clima_eventos_2026-10-26.csv"
            path.write_text(
                "cidade,temperatura,umidade,descricao,timestamp\n"
                "São Paulo,24.5,70,claro,nao-e-data\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "linha 2"):
                list(import_results.ler_eventos(path))


if __name__ == "__main__":
    unittest.main()
