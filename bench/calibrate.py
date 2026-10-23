#!/usr/bin/env python3
import argparse
import json
import os
import time

import run as R

PROXY = ["--mode", "sns", "--aws-endpoint", R.SNS_PROXY, "--topic-arn", R.TOPIC_ARN]
DIRECT = ["--mode", "sns", "--aws-endpoint", f"http://{R.AWS}:5000", "--topic-arn", R.TOPIC_ARN]
HTTP = ["--mode", "http", "--url", R.WEBHOOK]
SMALL, MEDIUM, LARGE = "460", "760", "3015"

QUICK = [("sns_batch10_small_c8", [*PROXY, "--payload", SMALL, "--concurrency", "8", "--duration", "5s"])]
FULL = [
    *[(f"sns_batch10_small_c{c}", [*PROXY, "--payload", SMALL, "--concurrency", c, "--duration", "10s"]) for c in ("1", "8", "32")],
    ("sns_batch10_small_c8_direct", [*DIRECT, "--payload", SMALL, "--concurrency", "8", "--duration", "10s"]),
    ("sns_batch10_medium_c8", [*PROXY, "--payload", MEDIUM, "--concurrency", "8", "--duration", "10s"]),
    ("sns_batch10_large_c8", [*PROXY, "--payload", LARGE, "--concurrency", "8", "--duration", "10s"]),
    ("sns_batch10_small_paced1000", [*PROXY, "--payload", SMALL, "--concurrency", "8", "--rate", "1000", "--duration", "10s"]),
    ("http_c1", [*HTTP, "--payload", SMALL, "--concurrency", "1", "--duration", "10s"]),
    ("http_c32", [*HTTP, "--payload", SMALL, "--concurrency", "32", "--duration", "10s"]),
    ("http_paced1000", [*HTTP, "--payload", SMALL, "--concurrency", "1", "--rate", "1000", "--duration", "10s"]),
]


def measure(args):
    R.recv_get("/reset")
    c = json.loads(R.benchctl(["calib", *args]))
    time.sleep(2)
    s = R.recv_get(f"/summary?expect={c['last_seq']}&min_seq=1")
    s.pop("missing_sample", None)
    c["recorded"] = s
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default=os.path.join(R.BENCH, "results"))
    a = ap.parse_args()
    rd = os.path.join(os.path.abspath(a.out), f"calibration-{'quick-' if a.quick else ''}{time.strftime('%Y%m%d-%H%M%S')}")
    os.makedirs(rd, exist_ok=True)
    R.ensure_pg()
    R.fresh_aws()
    R.start_recv()
    out = {"moto_image_id": R.sh(["docker", "image", "inspect", "-f", "{{.Id}}", R.MOTO]), "loadavg_start": os.getloadavg()}
    for key, args in QUICK if a.quick else FULL:
        c = out[key] = measure(args)
        r = c["recorded"]
        R.log(key, round(c["messages_per_s"]), "msg/s", round(c["calls_per_s"]), "calls/s", "call p50/p99", c["call_ms_p50"], c["call_ms_p99"],
              "recorded", r["distinct"], "of", c["messages"], "body", round(r["avg_body_bytes"]), "B")
    out["loadavg_end"] = os.getloadavg()
    R.rm(R.RECV, R.AWS)
    R.write(f"{rd}/calibration.json", json.dumps(out, indent=1))
    R.log("wrote", rd)


if __name__ == "__main__":
    main()
