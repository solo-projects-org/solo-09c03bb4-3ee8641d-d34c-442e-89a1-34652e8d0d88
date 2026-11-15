#!/usr/bin/env bash
# 复核组撤回手动样例：采集发布 -> 填写原因撤回所见修订 -> 旧修订被围栏挡住
# -> 县站更高修订发布后恢复。依赖 docker compose 默认端口
# （station 8001 / web1 8011 / web2 8012）。
set -euo pipefail

STATION="${STATION:-http://localhost:8001}"
WEB1="${WEB1:-http://localhost:8011}"
WEB2="${WEB2:-http://localhost:8012}"
# Unique section per invocation so repeated walkthroughs never share local DB
# state with a previous run (the station synthesizes data for any section).
SECTION="${SECTION:-WALKTHROUGH-$(date +%H%M%S)-$$}"
DATE="2026-11-10"
REASON="污染复核：仪器故障数据混入，县站尚未更正，先阻止下游取用"
echo "断面=$SECTION 日期=$DATE"

j() { python3 -m json.tool; }
wait_run() {
  local base="$1" rid="$2"
  for _ in $(seq 1 40); do
    local st
    st=$(curl -sS "$base/api/runs/$rid" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
    [ "$st" != "running" ] && { echo "$st"; return; }
    sleep 0.5
  done
  echo "timeout"
}

echo "== 1. 县站回到修订号 1，Web 采集并整日发布 =="
curl -sS -X POST "$STATION/admin/reset" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}" | j
RUN=$(curl -sS -X POST "$WEB1/api/collect" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}")
RUN_ID=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN_ID -> $(wait_run "$WEB1" "$RUN_ID")"
curl -sS "$WEB1/api/published?section=$SECTION&date=$DATE" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print('状态', d['status'], '修订号', d['revision'], '行数', len(d['rows']))"

echo "== 2. 旧页面用错误（过期）修订号撤回 -> 409 revision_conflict，新档不动 =="
curl -sS -o /tmp/wd_conflict.json -w "HTTP %{http_code}\n" \
  -X POST "$WEB1/api/withdraw" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\",\"expected_revision\":99,\"reason\":\"旧页面重复提交\"}"
cat /tmp/wd_conflict.json | j

echo "== 3. 在 web2 上用所见修订号 1 + 原因撤回 =="
curl -sS -X POST "$WEB2/api/withdraw" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\",\"expected_revision\":1,\"reason\":\"$REASON\"}" | j

echo "== 4. 两个 Web 实例读取：撤回状态 + 原因，零旧观测 =="
for base in "$WEB1" "$WEB2"; do
  curl -sS "$base/api/published?section=$SECTION&date=$DATE" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('$base ->', d['status'], '被撤回修订', d.get('withdrawn_revision'), '行数', len(d['rows']), '原因', d.get('reason'))"
done

echo "== 5. 重复撤回 -> 409 already_withdrawn =="
curl -sS -o /tmp/wd_again.json -w "HTTP %{http_code}\n" \
  -X POST "$WEB1/api/withdraw" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\",\"expected_revision\":1,\"reason\":\"重复撤回\"}"
cat /tmp/wd_again.json | j

echo "== 6. 同修订（或更早）在途采集迟到 -> 发布事务围栏拒绝 =="
RUN=$(curl -sS -X POST "$WEB1/api/collect" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}")
RUN_ID=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN_ID -> $(wait_run "$WEB1" "$RUN_ID")"
curl -sS "$WEB1/api/runs/$RUN_ID" | python3 -c "import sys,json;print('失败原因:', json.load(sys.stdin)['error'])"

echo "== 7. 县站更正（修订号 1 -> 2），更高修订通过整日校验后恢复 =="
curl -sS -X POST "$STATION/admin/correct" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\",\"time\":\"08:00\",\"nh3n\":6.321}" | j
RUN=$(curl -sS -X POST "$WEB2/api/collect" -H 'Content-Type: application/json' \
  -d "{\"section\":\"$SECTION\",\"date\":\"$DATE\"}")
RUN_ID=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN_ID -> $(wait_run "$WEB2" "$RUN_ID")"
for base in "$WEB1" "$WEB2"; do
  curl -sS "$base/api/published?section=$SECTION&date=$DATE" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('$base ->', d['status'], '修订号', d['revision'], '行数', len(d['rows']))"
done

echo "== 8. 撤回审计记录（含被更高修订取代的时间） =="
curl -sS "$WEB1/api/withdrawals?section=$SECTION&date=$DATE" | j
