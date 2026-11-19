"""Расчёт пар навыков из ДЗ 3 под координацией ZooKeeper.

Несколько копий координатора выбирают лидера, лидер считает куски CSV по одному
и после каждого кладёт отметку в чекпоинт. Если лидер пропал, следующий
продолжает с чекпоинта. Результат публикуется указателем /skills/result.

python coordinator.py reset
NAME=coord-a RUN=nofence python coordinator.py
NAME=coord-b RUN=fence FENCING=1 python coordinator.py
"""
import csv
import io
import itertools
import json
import logging
import os
import pickle
import sys
import time
from collections import Counter

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError

import lake
from mapreduce import KS, ONE_C_B, ROLE_B, read_header, read_split

HOSTS = os.environ.get("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")
SESSION_TIMEOUT = float(os.environ.get("ZK_TIMEOUT", "10"))
FENCING = os.environ.get("FENCING") == "1"
NAME = os.environ.get("NAME", f"coord-{os.getpid()}")
RUN = os.environ.get("RUN", "manual")
CHUNK_MB = int(os.environ.get("CHUNK_MB", "32"))
# без задержки весь расчёт занимает ~15 с, лидера не успеть приостановить посередине
SPLIT_DELAY = float(os.environ.get("SPLIT_DELAY", "1.0"))

ROOT = "/skills"
ELECTION = ROOT + "/election"
EPOCH = ROOT + "/epoch"
CHECKPOINT = ROOT + "/checkpoint"
RESULT = ROOT + "/result"
OUT = "results/zk"

log = logging.getLogger("coord")


def setup_log():
    os.makedirs(f"logs/{RUN}", exist_ok=True)
    fmt = logging.Formatter(f"%(asctime)s.%(msecs)03d {NAME} %(message)s", "%H:%M:%S")
    for h in (logging.FileHandler(f"logs/{RUN}/{NAME}.log", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def count_split(c, split, cols, size):
    start, end = split
    lines = read_split(c, lake.RAW_KEY, start, end, size).split(b"\n")
    if start == 0:
        lines = lines[1:]
    acc = Counter()
    for raw in lines:
        if not raw or ROLE_B not in raw or not ONE_C_B.search(raw):
            continue
        row = next(csv.reader([raw.decode("utf-8")]))
        if len(row) != len(cols) or not lake.is_variant(row[cols["role"]], row[cols["name"]]):
            continue
        skills = lake.skill_set(row[cols["key_skills"]])
        for k in KS:
            acc.update(itertools.combinations(skills, k))
    return acc


class Coordinator:
    def __init__(self):
        self.zk = KazooClient(hosts=HOSTS, timeout=SESSION_TIMEOUT)
        self.zk.add_listener(self.on_state)
        self.s3 = lake.s3()
        self.epoch = None

    def on_state(self, state):
        log.info(f"сессия ZooKeeper: {state}")

    def run(self):
        self.zk.start()
        log.info(f"подключён к {HOSTS}, сессия 0x{self.zk.client_id[0]:x}, "
                 f"тайм-аут {SESSION_TIMEOUT:.0f} с, FENCING={int(FENCING)}")
        log.info("жду лидерства")
        self.zk.Election(ELECTION, NAME).run(self.lead)
        self.zk.remove_listener(self.on_state)
        self.zk.stop()

    def lead(self):
        self.epoch = self.zk.retry(self.bump_epoch)
        log.info(f"стал лидером, эпоха {self.epoch}")
        if self.zk.retry(self.zk.get, RESULT)[0]:
            log.info("результат уже опубликован, выхожу")
            return
        size = self.s3.head_object(Bucket=lake.BUCKET, Key=lake.RAW_KEY)["ContentLength"]
        cols = read_header(self.s3, lake.RAW_KEY)
        chunk = CHUNK_MB << 20
        splits = [(s, min(s + chunk, size)) for s in range(0, size, chunk)]
        done = self.zk.retry(self.zk.get_children, CHECKPOINT)
        log.info(f"чекпоинт: готово {len(done)} из {len(splits)} кусков")

        skipped = 0
        for i, split in enumerate(splits):
            node = f"{CHECKPOINT}/split-{i:04d}"
            if self.zk.retry(self.zk.exists, node):
                skipped += 1
                continue
            t = time.perf_counter()
            acc = count_split(self.s3, split, cols, size)
            key = f"{OUT}/epoch-{self.epoch}/map/split-{i:04d}.pkl"
            self.s3.put_object(Bucket=lake.BUCKET, Key=key, Body=pickle.dumps(dict(acc)))
            try:
                self.zk.retry(self.zk.create, node, key.encode())
            except NodeExistsError:
                log.info(f"кусок {i} уже отмечен в чекпоинте другим лидером")
            log.info(f"кусок {i + 1}/{len(splits)}: {len(acc)} наборов за {time.perf_counter() - t:.2f} с")
            time.sleep(SPLIT_DELAY)
        log.info(f"map закончен, пропущено по чекпоинту {skipped} кусков")

        self.publish(self.reduce(len(splits)))

    def bump_epoch(self):
        _, st = self.zk.get(EPOCH)
        st = self.zk.set(EPOCH, NAME.encode(), version=st.version)
        return st.version

    def reduce(self, n_splits):
        acc = Counter()
        keys = sorted(self.zk.retry(self.zk.get_children, CHECKPOINT))
        assert len(keys) == n_splits, f"в чекпоинте {len(keys)} кусков из {n_splits}"
        for name in keys:
            key = self.zk.retry(self.zk.get, f"{CHECKPOINT}/{name}")[0].decode()
            acc.update(pickle.loads(self.s3.get_object(Bucket=lake.BUCKET, Key=key)["Body"].read()))
        prefix = f"{OUT}/epoch-{self.epoch}/result"
        summary = {}
        for k in KS:
            rows = sorted(((cmb, n) for cmb, n in acc.items() if len(cmb) == k),
                          key=lambda x: (-x[1], x[0]))
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow([f"skill_{j + 1}" for j in range(k)] + ["count"])
            w.writerows([*cmb, n] for cmb, n in rows)
            self.s3.put_object(Bucket=lake.BUCKET, Key=f"{prefix}/k{k}.csv", Body=buf.getvalue().encode())
            summary[f"k{k}"] = {"distinct": len(rows), "total": sum(n for _, n in rows)}
            log.info(f"k={k}: уникальных {len(rows)}, сумма частот {summary[f'k{k}']['total']}")
        return {"owner": NAME, "epoch": self.epoch, "prefix": prefix, **summary}

    def publish(self, pointer):
        body = json.dumps(pointer, ensure_ascii=False).encode()
        if not FENCING:
            st = self.zk.retry(self.zk.set, RESULT, body)
            log.info(f"записал результат: {RESULT} версия {st.version}, эпоха {self.epoch}")
            return
        tx = self.zk.transaction()
        tx.check(EPOCH, version=self.epoch)
        tx.set_data(RESULT, body)
        check, written = self.zk.retry(tx.commit)
        if isinstance(check, BadVersionError):
            current = self.zk.retry(self.zk.get, EPOCH)[1].version
            log.info(f"запись результата отклонена: BadVersionError, моя эпоха {self.epoch}, "
                     f"текущая {current}")
            return
        log.info(f"записал результат: {RESULT} версия {written.version}, эпоха {self.epoch}")


def reset():
    zk = KazooClient(hosts=HOSTS)
    zk.start()
    if zk.exists(ROOT):
        zk.delete(ROOT, recursive=True)
    for path in (ROOT, ELECTION, EPOCH, CHECKPOINT, RESULT):
        zk.create(path, b"")
    zk.stop()
    c = lake.s3()
    for obj in c.list_objects_v2(Bucket=lake.BUCKET, Prefix=OUT + "/").get("Contents", []):
        c.delete_object(Bucket=lake.BUCKET, Key=obj["Key"])
    print(f"{ROOT} создан заново, s3://{lake.BUCKET}/{OUT}/ очищен")


if __name__ == "__main__":
    if sys.argv[1:] == ["reset"]:
        reset()
    else:
        setup_log()
        Coordinator().run()
