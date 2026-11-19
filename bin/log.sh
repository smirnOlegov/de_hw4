# source bin/log.sh; LOG=out/xx.txt; run 'команда'
export PATH="$PWD/bin:$PWD/.venv/bin:$PATH"
run() { { echo "\$ $*"; bash -c "$*" 2>&1; echo; } | tee -a "$LOG"; }
