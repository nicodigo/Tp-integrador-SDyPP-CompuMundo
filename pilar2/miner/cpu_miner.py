"""CPU fallback miner — development substitute for the CUDA md5_range binary.

Same interface as md5_range:
    cpu_miner.py <base_string> <target_prefix> <range_min> <range_max>

Output on success:
    nonce = 12345
    MD5(<base_string>+12345) = 0000abcd...

Output on failure:
    No solution found

Exit codes: 0 (found or not found), non-zero only on argument errors.
"""

import hashlib
import sys


def main() -> None:
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <base_string> <target_prefix> <range_min> <range_max>",
              file=sys.stderr)
        sys.exit(2)

    base_string = sys.argv[1]
    target_prefix = sys.argv[2]
    range_min = int(sys.argv[3])
    range_max = int(sys.argv[4])

    for nonce in range(range_min, range_max + 1):
        digest = hashlib.md5((base_string + str(nonce)).encode()).hexdigest()
        if digest.startswith(target_prefix):
            print(f"nonce = {nonce}")
            print(f"MD5({base_string}+{nonce}) = {digest}")
            return

    print("No solution found")


if __name__ == "__main__":
    main()
