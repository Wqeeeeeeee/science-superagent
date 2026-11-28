#!/usr/bin/env python3
"""Trusted offline subprocess fixture; reads and writes only relative artifacts."""
import json
from pathlib import Path
import sys

request = json.loads(Path('input.json').read_text())
if request['kind'] == 'recovery':
    evidence = {'normalizer': 'decimal-v1'}
else:
    try:
        values = [int(value.strip() if request['mode'] == 'local-trim' else value,
                      10 if request['mode'] == 'decimal-v1' else 0) for value in request['data']]
    except ValueError:
        sys.exit(12)
    evidence = dict(count=len(values), total=sum(values), mean_milli=1000 * sum(values) // len(values))
result = dict(version=1, task_id=request['task_id'], attempt=request['attempt'], kind=request['kind'], evidence=evidence)
Path('result.json').write_text(json.dumps(result, sort_keys=True) + '\n')
