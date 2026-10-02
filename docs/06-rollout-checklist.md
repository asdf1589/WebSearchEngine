# 上線前要做的事

這個分支第一次部署到正式環境 (metricdb 172.16.191.1:5433) 時，依序做以下四件事。

## 0. 備份受升級影響的表

由部署的人在部署前執行。第 1 步的自動升級和第 2 步的手動修正會改到這些表：

- 6 張 coverage 表：`metric_headset_total`、`metric_headset_a`、`metric_headset_b`、`metric_randomset_total`、`metric_randomset_a`、`metric_randomset_b`（加欄位、改主鍵、`is_recheck` 分類、`indexed_*` / `ranked_*` 改 NULL）
- `metric_url`（加欄位、`is_indexed` 改 NULL）
- `crawler_stat_total`、`crawler_stat_a`、`crawler_stat_b`（`indexed` 的 0 改 NULL）

```bash
pg_dump -h 172.16.191.1 -p 5433 -U metric -d metricdb -Fc \
  -t metric_headset_total -t metric_headset_a -t metric_headset_b \
  -t metric_randomset_total -t metric_randomset_a -t metric_randomset_b \
  -t metric_url -t crawler_stat_total -t crawler_stat_a -t crawler_stat_b \
  -f metricdb_before_upgrade_$(date +%Y%m%d).dump
```

## 1. 部署新的映像檔

容器啟動時，`entrypoint.sh` 會先執行 `measure.py --createtable`。第一次升級時會做這些事（判斷方式：coverage 表的 `batch_id` 欄位是不是這次才新增的）：

- 6 張 coverage 表加上 `batch_id` / `measured_at` / `batch_age_days` / `is_recheck`，主鍵改成 `(batch_id, stat_date)`。
- 升級前寫入的列依規則分類 `is_recheck`：同一張表、同一個 batch 裡，`batch_age_days <= 2` 且 `stat_date` 最早的那一列是建立當日量測，其餘都是重量。每張表印出一行 `[migrate] ... legacy rows classified, N initial measurement(s), M re-measurement(s)`。
- 沒量過的 0 改成 NULL（2026-06-05 以前的 `indexed_*`、全部的 `ranked_*`、`crawler_stat_*.indexed`）。

升級失敗時容器會結束，不會啟動 cron；錯誤訊息在 `docker logs` 和 `/var/log/cron.log`。

之後容器重啟只會檢查 schema，不會再分類，也不會再改 NULL，所以第 2 步的手動修正不會被蓋掉。

## 2. 升級完成後，手動執行修正 SQL

必須在第 1 步之後執行（要有 `batch_id`、`is_recheck` 欄位），請用有寫入權限的帳號連 metricdb。6 張 coverage 表都要改：

- batch 1：2026-02-27 那次量測沒有量完（head 只有 49 條、random 只有 404 條；03-01 分別是 7,117 和 7,497 條）。依規則它會被分成建立當日量測，所以要手動改成重量，並把 2026-03-01 那列改成建立當日量測。
- 2026-09-17 以後的列：上線前的舊程式沒有接 selectdb，`indexed_num` / `indexed_rate` 一律寫 0（例如 batch 18 的 09-17、batch 19 的 head 10-02），但實際上沒有量 indexed。自動清理只處理 2026-06-05（第一次出現 indexed > 0）以前的列，所以用條件 `stat_date >= '2026-09-17' AND indexed_num = 0` 手動改成 NULL；這些 batch 在 `metric_url` 的 `is_indexed` 也一起改成 NULL。
  這個條件成立的前提：新程式在名單是空的時候寫 NULL 而不是 0；名單非空時，indexed 是「在名單裡而且已抓取」的 golden URL 數，每組有上千條 URL，實際上不會剛好是 0。所以 `indexed_num = 0` 的列只會是舊程式寫的。如果某一列真的量到 0，執行前先用下面第 3 步的查詢確認。

```sql
BEGIN;

-- batch 1：2026-02-27 那次量測沒有量完 (head 49 條、random 404 條)，改成重量；2026-03-01 那次才是建立當日量測
UPDATE metric_headset_total   SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_headset_a       SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_headset_b       SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_randomset_total SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_randomset_a     SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_randomset_b     SET is_recheck = true  WHERE batch_id = 1 AND stat_date = '2026-02-27';
UPDATE metric_headset_total   SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';
UPDATE metric_headset_a       SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';
UPDATE metric_headset_b       SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';
UPDATE metric_randomset_total SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';
UPDATE metric_randomset_a     SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';
UPDATE metric_randomset_b     SET is_recheck = false WHERE batch_id = 1 AND stat_date = '2026-03-01';

-- 2026-09-17 以後舊程式寫的列沒有量 indexed (寫死 0)，改成 NULL
-- metric_url 要先改：用 coverage 表還是 0 的列找出受影響的 batch
UPDATE metric_url u SET is_indexed = NULL
FROM metric_queries q
WHERE q.id = u.query_id AND q.batch_id IN (
          SELECT batch_id FROM metric_headset_total   WHERE stat_date >= '2026-09-17' AND indexed_num = 0
    UNION SELECT batch_id FROM metric_randomset_total WHERE stat_date >= '2026-09-17' AND indexed_num = 0
);
UPDATE metric_headset_total   SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;
UPDATE metric_headset_a       SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;
UPDATE metric_headset_b       SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;
UPDATE metric_randomset_total SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;
UPDATE metric_randomset_a     SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;
UPDATE metric_randomset_b     SET indexed_num = NULL, indexed_rate = NULL WHERE stat_date >= '2026-09-17' AND indexed_num = 0;

COMMIT;
```

## 3. 確認結果

```sql
-- batch 1：02-27 應為 true (重量)，03-01 應為 false (建立當日量測)，6 張表都一樣
SELECT 'headset_total' AS t, stat_date, is_recheck FROM metric_headset_total   WHERE batch_id = 1
UNION ALL SELECT 'headset_a',       stat_date, is_recheck FROM metric_headset_a       WHERE batch_id = 1
UNION ALL SELECT 'headset_b',       stat_date, is_recheck FROM metric_headset_b       WHERE batch_id = 1
UNION ALL SELECT 'randomset_total', stat_date, is_recheck FROM metric_randomset_total WHERE batch_id = 1
UNION ALL SELECT 'randomset_a',     stat_date, is_recheck FROM metric_randomset_a     WHERE batch_id = 1
UNION ALL SELECT 'randomset_b',     stat_date, is_recheck FROM metric_randomset_b     WHERE batch_id = 1
ORDER BY 1, 2;

-- 執行第 2 步之前：列出會被改成 NULL 的列，確認都是上線前舊程式寫的 (measured_at IS NULL)
SELECT 'headset_total' t, batch_id, stat_date, measured_at, indexed_num FROM metric_headset_total
 WHERE stat_date >= '2026-09-17' AND indexed_num = 0
UNION ALL SELECT 'randomset_total', batch_id, stat_date, measured_at, indexed_num FROM metric_randomset_total
 WHERE stat_date >= '2026-09-17' AND indexed_num = 0
ORDER BY 1, 3;

-- 執行第 2 步之後：兩個數字都應為 0
SELECT (SELECT count(*) FROM metric_headset_total   WHERE stat_date >= '2026-09-17' AND indexed_num = 0)
     + (SELECT count(*) FROM metric_randomset_total WHERE stat_date >= '2026-09-17' AND indexed_num = 0) AS coverage_left,
       (SELECT count(*) FROM metric_url u JOIN metric_queries q ON q.id = u.query_id
         WHERE q.batch_id IN (SELECT batch_id FROM metric_headset_total   WHERE stat_date >= '2026-09-17' AND indexed_num IS NULL AND measured_at IS NULL
                        UNION SELECT batch_id FROM metric_randomset_total WHERE stat_date >= '2026-09-17' AND indexed_num IS NULL AND measured_at IS NULL)
           AND u.is_indexed IS NOT NULL) AS metric_url_left;
```

每小時的 `migrate.py` 會整張重建 Neon 上的表，下一次執行後 Power BI 就會看到修正後的資料。
