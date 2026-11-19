# ДЗ 4. Отказоустойчивость средствами Apache ZooKeeper

Вариант: программист 1С, расчёт пар навыков из ДЗ 3.

Отчёт: [report/report.pdf](report/report.pdf). Выводы всех команд лежат в `out/`, журналы координаторов в `logs/`.

## Файлы

- `compose.yaml`: ансамбль из трёх ZooKeeper 3.9.6 (клиентские порты 2181, 2182, 2183) и RustFS из ДЗ 3 (S3 на 9100)
- `zk1/conf/zoo.cfg`, `zk2/conf/zoo.cfg`, `zk3/conf/zoo.cfg`: конфигурации серверов, `zkN/data/zkN/myid`: номер сервера
- `coordinator.py`: координатор расчёта с выбором лидера, чекпоинтом и защитой по эпохе (`FENCING=1`)
- `check.py`: сверка опубликованного результата с ДЗ 3
- `lake.py`, `mapreduce.py`, `upload.py`: из ДЗ 3
- `bin/zkcli`, `bin/zkstatus`: zkCli и `zkServer.sh status` внутри контейнеров
- `bin/switch_time.py`: время смены лидера по журналам
- `run_all.sh`: полный прогон

## Запуск

```bash
uv venv .venv && uv pip install --python .venv/bin/python -r requirements.txt
mkdir -p data && cp /путь/к/it_vacancies_2024.csv data/
docker compose up -d rustfs && .venv/bin/python upload.py data/it_vacancies_2024.csv
./run_all.sh
```

Отдельные этапы: `./run_all.sh ensemble`, `ephemeral`, `normal`, `nofence`, `fence`.
