import hashlib
import sys


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <base_string> <target_prefix>", file=sys.stderr)
        sys.exit(1)

    base = sys.argv[1]
    target = sys.argv[2]

    # Validate target prefix (lowercase hex only)
    for c in target:
        if c not in "0123456789abcdef":
            print("Target must be lowercase hex [0-9a-f]", file=sys.stderr)
            sys.exit(1)

    print(f"Base:   \"{base}\"")
    print(f"Target: \"{target}\"")

    nonce = 0
    while True:
        msg = f"{base}{nonce}".encode("ascii")
        digest = hashlib.md5(msg).hexdigest()

        if digest.startswith(target):
            print(f"\nFound!")
            print(f"  nonce = {nonce}")
            print(f"  MD5({base}{nonce}) = {digest}")
            sys.exit(0)

        nonce += 1


if __name__ == "__main__":
    main()
