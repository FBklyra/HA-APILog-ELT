"""Cross-check alelt_zlib_compress and alelt_sha256 against Python's zlib/hashlib.

Usage: python -I verify_zlib.py <path to aleltzlibcli.exe>
"""
import hashlib
import os
import random
import subprocess
import sys
import tempfile
import zlib


def main() -> int:
    exe = sys.argv[1]
    rnd = random.Random(1)
    cases = {
        "empty": b"",
        "one": b"a",
        "repeat": b"abcabcabc" * 10000,
        "random": os.urandom(200000),
        "mixed": b"".join(rnd.choice([b"INSERT APPSCHEMA.CUSTOMER ", bytes(rnd.randrange(256) for _ in range(7)),
                                      b"\x00" * 300, b"hello world "]) for _ in range(20000)),
        "big-stored": os.urandom(70000) * 2,
    }
    bad = 0
    with tempfile.TemporaryDirectory() as d:
        for name, data in cases.items():
            src, dst = os.path.join(d, "in.bin"), os.path.join(d, "out.z")
            with open(src, "wb") as f:
                f.write(data)
            out = subprocess.run([exe, src, dst], capture_output=True, text=True, check=True).stdout.split()
            with open(dst, "rb") as f:
                z = f.read()
            ok = zlib.decompress(z) == data and out[2] == hashlib.sha256(data).hexdigest()
            print(f"{name:12} {len(data):>8} -> {len(z):>8}  {'OK' if ok else 'FAIL'}")
            bad += not ok
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
