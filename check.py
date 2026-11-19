"""Сверка результата, на который указывает /skills/result, с ДЗ 3.

python check.py
"""
import csv
import hashlib
import io
import json

from kazoo.client import KazooClient

import lake
from coordinator import HOSTS, OUT, RESULT

# out/06_verify.txt из ДЗ 3
HW3 = {
    "k1": (2461, 77375, "57114099f0f14e5aae56ed2696b84719"),
    "k2": (35355, 265621, "777870608da7fc898ad0d039d8405754"),
    "k3": (228307, 804297, "bed7f565cdf4dea31bdf33056ef54f6d"),
}


def digest(body):
    r = csv.reader(io.StringIO(body))
    next(r)
    rows = {tuple(combo): n for *combo, n in r}
    h = hashlib.md5()
    for combo in sorted(rows):
        h.update(("\x1f".join(combo) + "\t" + rows[combo] + "\n").encode())
    return len(rows), sum(map(int, rows.values())), h.hexdigest()


def main():
    zk = KazooClient(hosts=HOSTS)
    zk.start()
    body, st = zk.get(RESULT)
    zk.stop()
    pointer = json.loads(body)
    print(f"{RESULT}: версия {st.version}, записал {pointer['owner']}, эпоха {pointer['epoch']}")
    c = lake.s3()
    keys = [o["Key"] for o in c.list_objects_v2(Bucket=lake.BUCKET, Prefix=OUT + "/")["Contents"]]
    prefixes = sorted({k.rsplit("/", 1)[0] for k in keys if "/result/" in k})
    ok = True
    for prefix in prefixes:
        mark = "  <- указатель" if prefix == pointer["prefix"] else ""
        print(f"\ns3://{lake.BUCKET}/{prefix}/{mark}")
        for k, want in HW3.items():
            got = digest(c.get_object(Bucket=lake.BUCKET, Key=f"{prefix}/{k}.csv")["Body"].read().decode())
            same = got == want
            ok &= same
            print(f"  {k}: уникальных {got[0]}, сумма частот {got[1]}, md5 {got[2]}  "
                  f"{'СОВПАДАЕТ с ДЗ 3' if same else f'НЕ СОВПАДАЕТ, в ДЗ 3 {want}'}")
    print("\nитог:", "все результаты совпадают с ДЗ 3" if ok else "РАСХОЖДЕНИЕ с ДЗ 3")


if __name__ == "__main__":
    main()
