import re
import unittest
from pathlib import Path


INDEX_HTML = Path(__file__).resolve().parents[1] / "web" / "index.html"


class FrontendDetailLoadingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = INDEX_HTML.read_text(encoding="utf-8")

    def test_compact_item_click_starts_lazy_detail_load(self):
        match = re.search(
            r"querySelectorAll\('\[data-compact\]'\).*?item\.onclick\s*=\s*\(\)\s*=>\s*\{(?P<body>.*?)\};",
            self.source,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        self.assertIn("loadSelectedDetail()", match.group("body"))

    def test_detail_loader_deduplicates_requests_and_checks_current_selection(self):
        match = re.search(
            r"function loadSelectedDetail\(\)\s*\{(?P<body>.*?)\n    \}",
            self.source,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("detailLoadPromises.get(videoId)", body)
        self.assertIn("ensureLibraryDetails([videoId])", body)
        self.assertIn("Number(selectedId) === videoId", body)

    def test_bulk_detail_loading_is_chunked(self):
        self.assertIn("const DETAIL_LOAD_BATCH = 200", self.source)
        match = re.search(
            r"async function ensureLibraryDetails\(videoIds\)\s*\{(?P<body>.*?)\n    \}",
            self.source,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("offset += DETAIL_LOAD_BATCH", body)
        self.assertIn("missingIds.slice(offset, offset + DETAIL_LOAD_BATCH)", body)


if __name__ == "__main__":
    unittest.main()
