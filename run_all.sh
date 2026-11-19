#!/bin/bash
# Полный прогон: ансамбль, потеря серверов, эфемерный узел, смена лидера без защиты и с защитой.
# ./run_all.sh [ensemble|ephemeral|normal|nofence|fence]
set -e
cd "$(dirname "$0")"
source bin/log.sh
mkdir -p out logs

leader() {
  for n in 1 2 3; do
    if docker exec zk$n zkServer.sh status 2>&1 | grep -q 'Mode: leader'; then echo zk$n; fi
  done
}

ensemble() {
  LOG=out/01_ensemble.txt; : > $LOG
  docker compose stop zk1 zk2 zk3 > /dev/null 2>&1
  run 'docker compose up -d 2>&1 | grep Started; until [ $(zkstatus | grep -c Mode) = 3 ]; do sleep 1; done'
  run 'docker compose ps --format "table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"'
  run 'docker exec zk1 bash -c "echo JAVA_HOME=\$JAVA_HOME; java -version 2>&1 | head -1"'
  run 'for n in 1 2 3; do docker inspect -f "{{.Name}}: {{join .Config.Cmd \" \"}}" zk$n; done'
  run 'for n in 1 2 3; do echo "--- zk$n/conf/zoo.cfg"; cat zk$n/conf/zoo.cfg; done'
  run 'diff zk1/conf/zoo.cfg zk2/conf/zoo.cfg; diff zk1/conf/zoo.cfg zk3/conf/zoo.cfg'
  run 'for n in 1 2 3; do echo "zk$n/data/zk$n/myid: $(cat zk$n/data/zk$n/myid)"; done'
  run 'for n in 1 2 3; do echo "--- zk$n"; docker exec zk$n zkServer.sh status 2>&1; done'
  run 'for n in 1 2 3; do echo "zk$n: $(docker exec zk$n bash -c "echo srvr | nc -q 1 localhost $((2180 + n))" | grep -E "Mode|Zxid|Node count" | tr "\n" " ")"; done'

  LOG=out/02_quorum.txt; : > $LOG
  first=$(leader)
  second=$( [ "$first" = zk1 ] && echo zk2 || echo zk1 )
  alive=$(for n in 1 2 3; do if [ zk$n != "$first" ] && [ zk$n != "$second" ]; then echo $n; fi; done)
  run 'zkstatus'
  run "docker compose stop $first 2>&1 | tail -1; sleep 5"
  run 'zkstatus'
  run "zkcli ${second#zk} create /quorum 'запись без $first'"
  run "zkcli $alive get /quorum"
  run "docker compose stop $second 2>&1 | tail -1; sleep 5"
  run 'zkstatus'
  run "docker exec zk$alive bash -c 'echo srvr | nc -q 1 localhost $((2180 + alive))'"
  run "time zkcli $alive get /quorum"
  run "docker compose start $first $second 2>&1 | tail -2; t=\$(date +%s); until [ \$(zkstatus | grep -c Mode) = 3 ]; do sleep 1; done; echo \"все три сервера в ансамбле через \$((\$(date +%s) - t)) с\""
  run 'zkstatus'
  run "zkcli ${first#zk} get /quorum"
  run "zkcli 1 delete /quorum"
}

ephemeral() {
  LOG=out/03_ephemeral.txt; : > $LOG
  run 'zkcli 1 ls /'
  # второе окно: интерактивный zkCli на zk2, команды идут через stdin, окно живёт до kill
  ( echo 'create -e /ephemeral "узел из второго окна"'; echo 'stat /ephemeral'; sleep 600 ) |
    docker exec -i zk2 zkCli.sh -timeout 10000 -server localhost:2182 > out/03_window2.raw 2>&1 &
  sleep 8
  grep -vE '^[0-9]{4}-[0-9]{2}-|^$|WATCHER::|WatchedEvent' out/03_window2.raw | sed 's/^/[окно 2] /' | tee -a $LOG
  echo | tee -a $LOG
  run 'zkcli 1 ls /'
  run 'zkcli 1 get -s /ephemeral | grep -E "узел|ephemeralOwner"'
  run 'docker exec zk2 pkill -9 -f ZooKeeperMain; t=$(date +%s.%N); echo "окно 2 закрыто в $(date +%T.%3N)"
zkcli 1 ls /
while zkcli 1 stat /ephemeral 2>&1 | grep -q ephemeralOwner; do sleep 0.5; done
echo "узел исчез в $(date +%T.%3N), через $(echo "$(date +%s.%N) - $t" | bc | cut -c1-4) с после закрытия окна"'
  run 'zkcli 1 ls /'
  run 'zkcli 1 stat /ephemeral'
  rm -f out/03_window2.raw
}

# два координатора, лидер coord-a; $1 имя запуска, $2 FENCING, $3 приостанавливать ли лидера
coordinators() {
  local run_name=$1 fencing=$2 pause=$3
  rm -rf logs/$run_name; mkdir -p logs/$run_name
  local slog=logs/$run_name/scenario.log
  note() { echo "$(date +%T.%3N) scenario $*" | tee -a $slog; }
  python coordinator.py reset
  NAME=coord-a RUN=$run_name FENCING=$fencing python coordinator.py > /dev/null 2>&1 & local a=$!
  sleep 2
  NAME=coord-b RUN=$run_name FENCING=$fencing python coordinator.py > /dev/null 2>&1 & local b=$!
  note "запущены coord-a (pid $a) и coord-b (pid $b), FENCING=$fencing"
  until grep -q 'кусок 8/' logs/$run_name/coord-a.log 2>/dev/null; do sleep 0.1; done
  grep -q 'стал лидером' logs/$run_name/coord-a.log
  zkcli 1 ls /skills/election | sed 's/^/election: /' | tee -a $slog
  if [ "$pause" = 1 ]; then
    kill -STOP $a
    note "лидер coord-a приостановлен: kill -STOP $a"
    wait $b
    note "coord-b завершился, возобновляю coord-a: kill -CONT $a"
    kill -CONT $a
  fi
  wait $a
  wait $b 2>/dev/null || true
  note "оба координатора завершились"
}

failover() {
  local run_name=$1 fencing=$2 pause=$3
  LOG=out/$4.txt; : > $LOG
  run "coordinators $run_name $fencing $pause"
  run "cat logs/$run_name/coord-a.log"
  run "cat logs/$run_name/coord-b.log"
  run "grep -h -E 'записал результат|отклонена' logs/$run_name/coord-*.log"
  if [ "$pause" = 1 ]; then
    run "python bin/switch_time.py logs/$run_name"
  fi
  run 'zkcli 1 get -s /skills/result | grep -E "owner|dataVersion|mtime"'
  run 'zkcli 1 get -s /skills/epoch | grep -E "^coord|dataVersion"; echo "кусков в чекпоинте: $(zkcli 1 ls /skills/checkpoint | tr "," "\n" | wc -l)"'
  run 'python check.py'
}

export -f coordinators
case "${1:-all}" in
  ensemble) ensemble ;;
  ephemeral) ephemeral ;;
  normal) failover normal 0 0 04_normal ;;
  nofence) failover nofence 0 1 05_nofence ;;
  fence) failover fence 1 1 06_fence ;;
  all)
    ensemble; ephemeral
    failover normal 0 0 04_normal
    failover nofence 0 1 05_nofence
    failover fence 1 1 06_fence ;;
esac
