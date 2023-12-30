import os
import sys
import json

defaults = ["Leader", "Follower", "Candidate"]


def work(fname, events):
    results = []
    with open(fname, "r") as f:
        data = f.read()
        data = data.rstrip()
        data = data.rstrip(",")
        data += "]"
        js = json.loads(data)
        for item in js:
            if item["name"].lower() in events:
                results.append(item)
        results.append(
            {
                "name": "End", 
                "pid": 0, 
                "ts": js[-1]["ts"] + 500 * 1000, 
                "tid": 0, 
                "ph": "i"
            }
        )
    with open(fname + ".new", "w") as f:
        f.write(json.dumps(results, indent=4))


if __name__ == "__main__":
    work(sys.argv[1], [s.lower() for s in sys.argv[2:] + defaults])