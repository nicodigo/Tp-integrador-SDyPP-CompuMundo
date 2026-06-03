"""CUDA miner worker — wraps the Pilar 1 binary as a callable service."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class MinerResult:
    """Successful Proof-of-Work solution returned by the CUDA miner."""

    nonce: int
    hash: str  # MD5 hex digest (32 chars), e.g. "0000004a7d..."

    def __repr__(self) -> str:
        return f"MinerResult(nonce={self.nonce}, hash={self.hash})"


class MinerError(Exception):
    """Raised when the CUDA miner subprocess fails (crashes, timeouts, etc.)."""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

_OUTPUT_RE = re.compile(
    r"^\s*nonce\s*=\s*(?P<nonce>\d+).*?"
    r"^.*?MD5\(.*?\)\s*=\s*(?P<hash>[0-9a-f]{32})",
    re.MULTILINE | re.DOTALL,
)

_NOT_FOUND_RE = re.compile(r"No solution found", re.IGNORECASE)


def _parse_miner_stdout(stdout: str) -> MinerResult | None:
    """Extract nonce + hash from the CUDA miner's stdout.

    Returns ``None`` if the miner explicitly reports *no solution found*,
    raises :class:`MinerError` if the output is unparseable.
    """
    if _NOT_FOUND_RE.search(stdout):
        return None

    m = _OUTPUT_RE.search(stdout)
    if not m:
        raise MinerError(f"Could not parse miner output:\n{stdout}")
    return MinerResult(nonce=int(m.group("nonce")), hash=m.group("hash"))


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class MinerService:
    """Wraps the Pilar 1 CUDA miner binary (``md5_range``) as a Python service.

    Usage::

        svc = MinerService(binary_path="./md5_range")
        result = svc.mine(block_fingerprint, "0000", 0, 10_000_000)
    """

    def __init__(
        self,
        binary_path: str = "./md5_range",
        timeout_seconds: int = 300,
    ) -> None:
        self.binary_path = binary_path
        self.timeout_seconds = timeout_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mine(
        self,
        base_string: str,
        target_prefix: str,
        range_min: int = 0,
        range_max: int = 1_000_000_000,
    ) -> MinerResult | None:
        """Run a single mining task and return the first valid nonce found.

        Parameters
        ----------
        base_string:
            The block fingerprint (SHA-256 hex) that workers hash against.
        target_prefix:
            Hex prefix the MD5 must start with, e.g. ``"0000"``.
        range_min:
            Inclusive start of nonce search space.
        range_max:
            Inclusive end of nonce search space.

        Returns
        -------
        MinerResult | None
            The winning (nonce, hash) pair, or ``None`` if no valid nonce
            exists in the given range.

        Raises
        ------
        MinerError
            If the subprocess crashes, times out, or produces unparseable output.
        """
        cmd = [
            self.binary_path,
            base_string,
            target_prefix,
            str(range_min),
            str(range_max),
        ]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise MinerError(
                f"Miner timed out after {self.timeout_seconds}s "
                f"(range [{range_min}, {range_max}])"
            ) from exc
        except FileNotFoundError as exc:
            raise MinerError(
                f"Miner binary not found at '{self.binary_path}'"
            ) from exc

        # Check for runtime errors (e.g. CUDA init failure)
        if proc.returncode not in (0, 1):
            raise MinerError(
                f"Miner exited with code {proc.returncode}:\n{proc.stderr}"
            )

        return _parse_miner_stdout(proc.stdout)
