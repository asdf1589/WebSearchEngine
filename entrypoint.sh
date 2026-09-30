#!/bin/sh
# 容器啟動腳本 (Dockerfile CMD)

# 1. metricdb schema 升級 (可重複執行，已升級過不會再改)
#    失敗時不啟動 cron：舊 schema 上跑新程式會寫入失敗或寫出錯的資料。
#    錯誤印到 stderr，docker logs 看得到；完整輸出在 /var/log/cron.log。
if ! (cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --createtable --metric_db_url 172.16.191.1:5433 >> /var/log/cron.log 2>&1); then
    echo "[entrypoint] metricdb schema upgrade failed; cron not started. Last lines of /var/log/cron.log:" >&2
    tail -n 30 /var/log/cron.log >&2
    exit 1
fi

# 2. 啟動 cron 並持續輸出日誌，防止容器停止
cron && exec tail -f /var/log/cron.log
