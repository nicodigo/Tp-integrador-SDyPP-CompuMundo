"""Unit tests for MinerService (mock subprocess)."""

import subprocess
import unittest
from unittest.mock import MagicMock, patch

from shared.miner import (
    MinerError,
    MinerResult,
    MinerService,
    _parse_miner_stdout,
)

# ---------------------------------------------------------------------------
# Output parser (pure function — no subprocess needed)
# ---------------------------------------------------------------------------


class TestParseStdout(unittest.TestCase):
    def test_found_output(self):
        stdout = """\
Base:   "abc123"
Target: "0000"
Range:  [0, 999999]  (1000000 nonces)
Grid:   1280 blocks x 256 threads  (stride = 327680)

Found!
  nonce = 52776832
  MD5(abc12352776832) = 0000004a7d1c8b3e9f2a5c6d7e8f9a0b
"""
        result = _parse_miner_stdout(stdout)
        self.assertIsNotNone(result)
        assert result is not None  # type-narrow
        self.assertEqual(result.nonce, 52776832)
        self.assertEqual(result.hash, "0000004a7d1c8b3e9f2a5c6d7e8f9a0b")

    def test_not_found_output(self):
        stdout = """\
Base:   "abc123"
Target: "000000"
Range:  [0, 999999]  (1000000 nonces)
Grid:   1280 blocks x 256 threads  (stride = 327680)

No solution found in range [0, 999999]
"""
        result = _parse_miner_stdout(stdout)
        self.assertIsNone(result)

    def test_unparseable_output(self):
        with self.assertRaises(MinerError):
            _parse_miner_stdout("Some garbage output")


# ---------------------------------------------------------------------------
# MinerService (subprocess mocked)
# ---------------------------------------------------------------------------


def _fake_completed(stdout: str, retcode: int = 0) -> MagicMock:
    """Create a mock subprocess.CompletedProcess."""
    proc = MagicMock(spec=subprocess.CompletedProcess)
    proc.returncode = retcode
    proc.stdout = stdout
    proc.stderr = ""
    return proc


class TestMinerService(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MinerService(binary_path="./fake_md5_range")

    @patch("subprocess.run")
    def test_mine_found(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _fake_completed(
            "Found!\n  nonce = 42\n  MD5(test42) = 0000004a7d1c8b3e9f2a5c6d7e8f9a0b\n",
        )

        result = self.service.mine("test", "0000", 0, 100)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.nonce, 42)
        self.assertEqual(result.hash, "0000004a7d1c8b3e9f2a5c6d7e8f9a0b")

        mock_run.assert_called_once_with(
            ["./fake_md5_range", "test", "0000", "0", "100"],
            capture_output=True,
            text=True,
            timeout=300,
        )

    @patch("subprocess.run")
    def test_mine_not_found(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _fake_completed(
            "No solution found in range [0, 999999]\n",
            retcode=1,
        )
        result = self.service.mine("test", "000000", 0, 999999)
        self.assertIsNone(result)

    @patch("subprocess.run")
    def test_mine_subprocess_crash(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _fake_completed("", retcode=2)
        with self.assertRaises(MinerError):
            self.service.mine("test", "0000")

    @patch("subprocess.run")
    def test_mine_timeout(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=[], timeout=1)
        with self.assertRaises(MinerError):
            self.service.mine("test", "0000")


if __name__ == "__main__":
    unittest.main()
