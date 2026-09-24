#!/bin/sh
# 容器啟動腳本 (Dockerfile CMD)

# 1. metricdb schema 升級 (可重複執行，已升級過不會再改)
cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --createtable --metric_db_url 172.16.191.1:5433 >> /var/log/cron.log 2>&1

# 2. cron 不會繼承 docker run -e 傳入的環境變數，寫進 /etc/environment 讓 migrate.py 讀得到
if [ -n "$NEON_URL" ]; then
    grep -v '^NEON_URL=' /etc/environment > /etc/environment.tmp 2>/dev/null
    echo "NEON_URL=$NEON_URL" >> /etc/environment.tmp
    cat /etc/environment.tmp > /etc/environment && rm -f /etc/environment.tmp
fi

# 3. 啟動 cron 並持續輸出日誌，防止容器停止
cron && exec tail -f /var/log/cron.log
