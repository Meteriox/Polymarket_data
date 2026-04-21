#!/usr/bin/env python3
"""Inspect orderfilled parquet file schema and sample values."""
import sys
from pathlib import Path
import pyarrow.parquet as pq

search_dirs = [
    Path('/app/data'),
    Path('/app/data/dataset'),
    Path('data'),
    Path('data/dataset'),
]

patterns = ['orderfilled.parquet', 'orderfilled_part1.parquet']

found = None
for d in search_dirs:
    if not d.exists():
        continue
    for pat in patterns:
        for p in sorted(d.glob(pat)):
            if p.is_file():
                found = p
                break
        if found:
            break
    if found:
        break

if not found:
    print("No orderfilled parquet file found")
    sys.exit(1)

print(f"File: {found}")
print(f"Size: {found.stat().st_size / 1024 / 1024:.1f} MB")

pf = pq.ParquetFile(str(found))
print(f"\nRow groups: {pf.metadata.num_row_groups}")
print(f"Total rows: {pf.metadata.num_rows}")
print(f"\n=== Arrow Schema ===")
for i, field in enumerate(pf.schema_arrow):
    print(f"  [{i:2d}] {field.name:30s} {field.type}")

print(f"\n=== First 3 rows of row group 0 ===")
t = pf.read_row_group(0)
for row_idx in range(min(3, t.num_rows)):
    print(f"\n--- Row {row_idx} ---")
    for col_name in t.column_names:
        val = t.column(col_name)[row_idx].as_py()
        if isinstance(val, bytes):
            n = int.from_bytes(val, 'big', signed=False)
            print(f"  {col_name:30s} = BINARY({len(val)}b) hex={val.hex()[:40]}... decimal={n}")
        else:
            print(f"  {col_name:30s} = {type(val).__name__:8s} {repr(val)[:80]}")
