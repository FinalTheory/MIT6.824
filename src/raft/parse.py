import os
import sys
import json

defaults = ["Leader", "Follower", "Candidate", "End"]


def work(fname, events):
    entries = [s for s in events if s.isdigit() or ' ' in s]
    results = []
    def filter_by_entry(item):
        if len(entries) == 0:
            return True
        if "args" not in item or item["args"] is None:
            return True
        if "args.Entries" not in item["args"] and "entry" not in item["args"] and "entries" not in item["args"]:
            return True
        for e in entries:
            if e in json.dumps(item):
                return True
        return False

    with open(fname, "r") as f:
        data = f.read()
        try:
            js = json.loads(data)
        except:
            data = data.rstrip()
            data = data.rstrip(",")
            data += "]"
            js = json.loads(data)
        for item in js:
            if item["name"].lower() in events and filter_by_entry(item):
                results.append(item)
        if js[-1]["name"] != "End":
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
        f.write(json.dumps(results))


if __name__ == "__main__":
    work(sys.argv[1], [s.lower() for s in sys.argv[2:] + defaults])