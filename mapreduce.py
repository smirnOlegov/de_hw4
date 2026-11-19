"""Свой MapReduce поверх RustFS.

Входной CSV читается из бакета кусками (HTTP Range), каждый кусок обрабатывает
отдельный процесс-маппер. Маппер отбирает вакансии варианта и выдаёт пары
(набор навыков, 1) для отдельных навыков, пар и троек. Выход маппера
раскладывается по R партициям и сбрасывается на локальный диск, редьюсер
собирает свою партицию со всех мапперов, суммирует и пишет результат в бакет.

python mapreduce.py --workers 8 --chunk-mb 32 --reducers 4
python mapreduce.py --workers 1 --no-combiner --no-prefilter      # базовый вариант
python mapreduce.py --fail-map 2 5                                # отказ мапперов
"""
import argparse
import csv
import io
import itertools
import json
import multiprocessing as mp
import multiprocessing.connection
import os
import pickle
import re
import shutil
import signal
import sys
import tempfile
import time
import zlib
from collections import Counter, deque

import lake

TAIL = 256 << 10            # шаг дочитывания последней строки куска
KS = (1, 2, 3)              # размеры наборов: навык, пара, тройка
ROLE_B = lake.ROLE.encode()
# грубый фильтр по байтам строки: «1С», «1с», «1C», «1c», «1 С»...
ONE_C_B = re.compile(rb"1 ?(?:\xd1\x81|\xd0\xa1|c|C)")

csv.field_size_limit(sys.maxsize)


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def get_range(c, key, lo, hi):
    r = c.get_object(Bucket=lake.BUCKET, Key=key, Range=f"bytes={lo}-{hi}")
    return r["Body"].read()


def read_split(c, key, start, end, size):
    """Байты строк, которые начинаются внутри [start, end).

    Строка, начатая в предыдущем куске, пропускается (её дочитает предыдущий
    маппер), а последняя строка куска дочитывается из следующего.
    """
    if start > 0:
        data = get_range(c, key, start - 1, end - 1)
        nl = data.find(b"\n")
        data = data[nl + 1:] if nl >= 0 else b""
        if not data:
            return b""
    else:
        data = get_range(c, key, 0, end - 1)
    pos = end
    while pos < size and not data.endswith(b"\n"):
        piece = get_range(c, key, pos, min(pos + TAIL, size) - 1)
        i = piece.find(b"\n")
        if i >= 0:
            data += piece[:i + 1]
            break
        data += piece
        pos += len(piece)
    return data


def partition(combo, r):
    # hash() у строк в Python свой в каждом процессе, поэтому crc32
    return zlib.crc32("\x1f".join(combo).encode()) % r


# ---------------------------------------------------------------- map

def map_task(task, attempt, split, cols, opt):
    start, end = split
    adir = os.path.join(opt.tmp, f"map-{task:04d}.try{attempt}")
    os.makedirs(adir)
    t0 = time.perf_counter()
    c = lake.s3()
    data = read_split(c, opt.key, start, end, opt.size)

    st = Counter(bytes=len(data))
    out = [Counter() for _ in range(opt.reducers)] if opt.combiner else \
          [[] for _ in range(opt.reducers)]
    i_role, i_name, i_skills = cols["role"], cols["name"], cols["key_skills"]

    lines = data.split(b"\n")
    if start == 0:
        lines = lines[1:]              # заголовок CSV
    crash_at = len(lines) // 2 if (task in opt.fail_map and attempt == 1) else -1

    for n, raw in enumerate(lines):
        if n == crash_at:
            # имитация падения процесса (OOM killer, kill -9): выход без коммита
            log(f"map {task}: попытка {attempt} аварийно завершена на строке {n}")
            os.kill(os.getpid(), signal.SIGKILL)
        if not raw:
            continue
        st["lines"] += 1
        if opt.prefilter and (ROLE_B not in raw or not ONE_C_B.search(raw)):
            continue
        row = next(csv.reader([raw.decode("utf-8")]))
        if len(row) != len(cols):
            st["bad_rows"] += 1
            continue
        if not lake.is_variant(row[i_role], row[i_name]):
            continue
        st["vacancies"] += 1
        skills = lake.skill_set(row[i_skills])
        if not skills:
            continue
        st["with_skills"] += 1
        for k in KS:
            for combo in itertools.combinations(skills, k):
                st[f"emit_k{k}"] += 1
                p = partition(combo, opt.reducers)
                if opt.combiner:
                    out[p][combo] += 1
                else:
                    out[p].append((combo, 1))

    for p, part in enumerate(out):
        with open(os.path.join(adir, f"part-{p:03d}.pkl"), "wb") as f:
            pickle.dump(dict(part) if opt.combiner else part, f, protocol=pickle.HIGHEST_PROTOCOL)
        st["shuffle_records"] += len(part)
    st["shuffle_bytes"] = sum(os.path.getsize(os.path.join(adir, x)) for x in os.listdir(adir))
    st["sec"] = round(time.perf_counter() - t0, 3)
    with open(os.path.join(adir, "stats.json"), "w") as f:
        json.dump(st, f)
    # коммит попытки: атомарное переименование каталога
    os.rename(adir, os.path.join(opt.tmp, f"map-{task:04d}"))


# ---------------------------------------------------------------- reduce

def reduce_task(task, attempt, n_maps, opt):
    t0 = time.perf_counter()
    acc = Counter()
    for m in range(n_maps):
        with open(os.path.join(opt.tmp, f"map-{m:04d}", f"part-{task:03d}.pkl"), "rb") as f:
            part = pickle.load(f)
        if opt.combiner:
            acc.update(part)
        else:
            for combo, one in part:
                acc[combo] += one

    c = lake.s3()
    summary = {}
    for k in KS:
        rows = sorted(((combo, n) for combo, n in acc.items() if len(combo) == k),
                      key=lambda x: (-x[1], x[0]))
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow([f"skill_{i + 1}" for i in range(k)] + ["count"])
        for combo, n in rows:
            w.writerow([*combo, n])
        c.put_object(Bucket=lake.BUCKET, Key=f"{opt.out}/k{k}/part-{task:05d}.csv",
                     Body=buf.getvalue().encode())
        summary[k] = {"distinct": len(rows), "total": sum(n for _, n in rows),
                      "top": [[list(cmb), n] for cmb, n in rows[:10]]}
    with open(os.path.join(opt.tmp, f"reduce-{task:03d}.json"), "w") as f:
        json.dump({"summary": summary, "sec": round(time.perf_counter() - t0, 3)}, f)


# ---------------------------------------------------------------- планировщик

def run_phase(name, n_tasks, target, opt, done, cleanup=None):
    """Запускает задачи в отдельных процессах, упавшие перезапускает."""
    ctx = mp.get_context("fork")
    pending = deque((t, 1) for t in range(n_tasks))
    running = {}
    failed = 0
    while pending or running:
        while pending and len(running) < opt.workers:
            t, a = pending.popleft()
            p = ctx.Process(target=target, args=(t, a), name=f"{name}-{t}")
            p.start()
            running[p.sentinel] = (p, t, a)
        for s in mp.connection.wait(list(running)):
            p, t, a = running.pop(s)
            p.join()
            if p.exitcode == 0 and done(t):
                continue
            failed += 1
            log(f"{name} {t}: попытка {a} упала (exitcode {p.exitcode})")
            if cleanup:
                cleanup(t, a)
            if a >= opt.max_attempts:
                raise SystemExit(f"{name} {t}: исчерпаны {opt.max_attempts} попытки")
            log(f"{name} {t}: перезапуск, попытка {a + 1}")
            pending.append((t, a + 1))
    return failed


def drop_attempt(tmp, task, attempt):
    """Выход упавшей попытки не закоммичен и в reduce не попадёт -- удаляем."""
    adir = os.path.join(tmp, f"map-{task:04d}.try{attempt}")
    if os.path.isdir(adir):
        log(f"map {task}: удалён незакоммиченный каталог {os.path.basename(adir)} "
            f"({len(os.listdir(adir))} файлов)")
        shutil.rmtree(adir)


def read_header(c, key):
    head = get_range(c, key, 0, 64 << 10)
    names = next(csv.reader([head.split(b"\n", 1)[0].decode()]))
    return {n: i for i, n in enumerate(names)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", default=lake.RAW_KEY)
    ap.add_argument("--out", default="results/mapreduce")
    ap.add_argument("--chunk-mb", type=int, default=32)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--no-combiner", dest="combiner", action="store_false")
    ap.add_argument("--no-prefilter", dest="prefilter", action="store_false")
    ap.add_argument("--fail-map", type=int, nargs="*", default=[])
    ap.add_argument("--max-attempts", type=int, default=3)
    opt = ap.parse_args()

    t_start = time.perf_counter()
    c = lake.s3()
    opt.size = c.head_object(Bucket=lake.BUCKET, Key=opt.key)["ContentLength"]
    cols = read_header(c, opt.key)
    chunk = opt.chunk_mb << 20
    splits = [(s, min(s + chunk, opt.size)) for s in range(0, opt.size, chunk)]
    opt.tmp = tempfile.mkdtemp(prefix="mr-")
    opt.fail_map = set(opt.fail_map)
    log(f"вход s3://{lake.BUCKET}/{opt.key}: {opt.size} байт, {len(splits)} кусков по {opt.chunk_mb} МиБ")
    log(f"workers={opt.workers} reducers={opt.reducers} combiner={opt.combiner} prefilter={opt.prefilter}")

    for old in c.list_objects_v2(Bucket=lake.BUCKET, Prefix=opt.out + "/").get("Contents", []):
        c.delete_object(Bucket=lake.BUCKET, Key=old["Key"])

    try:
        t = time.perf_counter()
        fm = run_phase("map", len(splits),
                       lambda i, a: map_task(i, a, splits[i], cols, opt), opt,
                       lambda i: os.path.isdir(os.path.join(opt.tmp, f"map-{i:04d}")),
                       lambda i, a: drop_attempt(opt.tmp, i, a))
        t_map = time.perf_counter() - t
        st = Counter()
        for i in range(len(splits)):
            with open(os.path.join(opt.tmp, f"map-{i:04d}", "stats.json")) as f:
                st.update({k: v for k, v in json.load(f).items() if k != "sec"})
        log(f"map: {len(splits)} задач за {t_map:.1f} с, перезапусков {fm}")

        t = time.perf_counter()
        fr = run_phase("reduce", opt.reducers,
                       lambda r, a: reduce_task(r, a, len(splits), opt), opt,
                       lambda r: os.path.exists(os.path.join(opt.tmp, f"reduce-{r:03d}.json")))
        t_red = time.perf_counter() - t
        log(f"reduce: {opt.reducers} задач за {t_red:.1f} с, перезапусков {fr}")

        res = {k: {"distinct": 0, "total": 0, "top": []} for k in KS}
        for r in range(opt.reducers):
            with open(os.path.join(opt.tmp, f"reduce-{r:03d}.json")) as f:
                part = json.load(f)["summary"]
            for k in KS:
                res[k]["distinct"] += part[str(k)]["distinct"]
                res[k]["total"] += part[str(k)]["total"]
                res[k]["top"] += part[str(k)]["top"]
    finally:
        shutil.rmtree(opt.tmp, ignore_errors=True)

    for k in KS:
        res[k]["top"] = sorted(res[k]["top"], key=lambda x: (-x[1], x[0]))[:10]
    summary = {"input_bytes": opt.size, "splits": len(splits), "stats": dict(st),
               "map_sec": round(t_map, 2), "reduce_sec": round(t_red, 2),
               "map_retries": fm, "results": res}
    c.put_object(Bucket=lake.BUCKET, Key=f"{opt.out}/summary.json",
                 Body=json.dumps(summary, ensure_ascii=False, indent=1).encode())
    c.put_object(Bucket=lake.BUCKET, Key=f"{opt.out}/_SUCCESS", Body=b"")

    print()
    print(f"прочитано из S3: {st['bytes']} байт, строк {st['lines']}, битых строк {st['bad_rows']}")
    print(f"вакансий варианта: {st['vacancies']}, из них с навыками: {st['with_skills']}")
    print(f"выход map: {sum(st[f'emit_k{k}'] for k in KS)} пар (набор, 1); в shuffle "
          f"{'после комбайнера ' if opt.combiner else ''}ушло {st['shuffle_records']} записей, "
          f"{st['shuffle_bytes']} байт")
    names = {1: "навыки", 2: "пары", 3: "тройки"}
    for k in KS:
        print(f"\nk={k} ({names[k]}): уникальных {res[k]['distinct']}, сумма частот {res[k]['total']}")
        for combo, n in res[k]["top"]:
            print(f"  {n:6d}  {' + '.join(combo)}")
    print(f"\nрезультат: s3://{lake.BUCKET}/{opt.out}/k{{1,2,3}}/part-*.csv")
    print(f"время: всего {time.perf_counter() - t_start:.1f} с (map {t_map:.1f} с, reduce {t_red:.1f} с)")


if __name__ == "__main__":
    main()
