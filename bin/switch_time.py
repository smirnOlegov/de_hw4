"""Время смены лидера по журналам одного запуска: python bin/switch_time.py logs/nofence"""
import sys
from datetime import datetime, timedelta


def when(path, text):
    for line in open(path, encoding="utf-8"):
        if text in line:
            return datetime.strptime(line.split()[0], "%H:%M:%S.%f"), line.strip()
    raise SystemExit(f"в {path} нет строки «{text}»")


def since(start, end):
    # в журналах только время суток, запуск может перейти через полночь
    return ((end - start) % timedelta(days=1)).total_seconds()


d = sys.argv[1]
paused, l1 = when(f"{d}/scenario.log", "приостановлен")
elected, l2 = when(f"{d}/coord-b.log", "стал лидером")
resumed, l3 = when(f"{d}/scenario.log", "возобновляю")
for line in (l1, l2, l3):
    print(line)
print(f"смена лидера: {since(paused, elected):.1f} с от приостановки coord-a до лидерства coord-b")
print(f"coord-a стоял на паузе {since(paused, resumed):.1f} с")
