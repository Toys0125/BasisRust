#!/usr/bin/env python3
"""Create a deterministic, valid 2048x2048 JPEG of exactly 5 MiB (requires Pillow)."""
import argparse
import hashlib
import io
import json
import pathlib
import random

from PIL import Image


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output must be a new file')
    target = 5 * 1024 * 1024
    image = Image.frombytes('RGB', (2048, 2048), random.Random(34).randbytes(2048 * 2048 * 3))
    buffer = io.BytesIO()
    image.save(buffer, format='JPEG', quality=74, subsampling=0)
    encoded = buffer.getvalue()
    if len(encoded) > target or target - len(encoded) < 4:
        parser.error('this Pillow encoder did not leave room for the JPEG comment padding')
    # COM markers are valid JPEG metadata, inserted immediately before the EOI marker.
    remaining = target - len(encoded)
    comments = bytearray()
    while remaining:
        size = min(remaining, 65537)  # two-byte marker + two-byte length + <=65533 data
        if 0 < remaining - size < 4:
            size -= 4
        comments.extend(b'\xff\xfe' + (size - 2).to_bytes(2, 'big') + bytes(size - 4))
        remaining -= size
    result = encoded[:-2] + comments + encoded[-2:]
    with Image.open(io.BytesIO(result)) as decoded:
        decoded.load()
        assert decoded.size == (2048, 2048)
    assert len(result) == target
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(result)
    print(json.dumps({'file_bytes': len(result), 'jpeg_bytes': len(encoded),
                      'comment_bytes': len(comments), 'sha256': hashlib.sha256(result).hexdigest()}))


if __name__ == '__main__':
    main()
