"""Load generator for the final lab: NDJSON log events to stdout (pipe into kafka-console-producer).

Mix: ~5% invalid ({"ts":"bad"}), INFO/ERROR/DEBUG levels, every event carries a secret (user.pwd).
Usage: python3 produce_logs.py [count]   (default 100000)
"""
import json
import random
import sys
import time

count = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
for i in range(count):
    if random.random() < 0.05:
        print('{"ts":"bad"}')
    else:
        print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                          "level": random.choice(["INFO", "ERROR", "DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
