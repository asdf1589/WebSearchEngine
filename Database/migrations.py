"""
metricdb 的 schema 升級。

`create_all` 只會建立不存在的表，不會改已經存在的表，所以既有的表要靠這裡補欄位。
每一步都可以重複執行：已經升級過的表不會再被改動。

執行方式 (容器啟動時會自動跑一次，見 entrypoint.sh)：
    python3 measure.py --createtable --metric_db_url 172.16.191.1:5433
"""
from sqlalchemy import text

SET_TYPES = ("headset", "randomset")
SUFFIXES = ("total", "a", "b")
COVERAGE_TABLES = [f"metric_{s}_{x}" for s in SET_TYPES for x in SUFFIXES]
CRAWLER_STAT_TABLES = [f"crawler_stat_{x}" for x in SUFFIXES]

METRIC_URL_COLUMNS = (
    ("url_canonical", "TEXT"),
    ("first_seen", "TIMESTAMPTZ"),
    ("last_scheduled", "TIMESTAMPTZ"),
    ("source", "SMALLINT"),
    ("robots_bits", "SMALLINT"),
    ("last_fail_reason", "TEXT"),
    ("num_scheduled_90d", "INTEGER"),
    ("num_fetch_fail_90d", "INTEGER"),
)

COVERAGE_COLUMNS = (
    ("batch_id", "BIGINT"),
    ("measured_at", "TIMESTAMP"),
    ("batch_age_days", "INTEGER"),
    ("is_recheck", "BOOLEAN NOT NULL DEFAULT FALSE"),
)


def _table_exists(conn, table):
    return conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": f"public.{table}"}).scalar()


def _column_exists(conn, table, column):
    return conn.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
        ),
        {"t": table, "c": column},
    ).first() is not None


def _primary_key(conn, table):
    """回傳 (constraint 名稱, [欄位...])，沒有主鍵時回傳 (None, [])。"""
    rows = conn.execute(
        text(
            """
            SELECT c.conname, a.attname
            FROM pg_constraint c
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.conrelid = CAST(:t AS regclass) AND c.contype = 'p'
            ORDER BY array_position(c.conkey, a.attnum)
            """
        ),
        {"t": f"public.{table}"},
    ).all()
    if not rows:
        return None, []
    return rows[0][0], [r[1] for r in rows]


def _migrate_coverage_table(conn, table):
    """
    加 batch_id / measured_at / batch_age_days / is_recheck，主鍵改成 (batch_id, stat_date)。
    回傳 True 表示這張表是第一次升級 (原本沒有 batch_id)。
    """
    first_time = not _column_exists(conn, table, "batch_id")

    for col, col_type in COVERAGE_COLUMNS:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}"))

    if first_time:
        # 舊資料沒有記 batch。cron 是 --create 之後馬上 --test，量的一定是當天為止最新的 batch。
        conn.execute(text(f"""
            UPDATE {table} c
            SET batch_id = (
                SELECT max(b.id) FROM metric_batches b
                WHERE b.created_at < c.stat_date + 1
            )
            WHERE c.batch_id IS NULL
        """))
        orphan = conn.execute(text(f"SELECT count(*) FROM {table} WHERE batch_id IS NULL")).scalar()
        if orphan:
            print(f"[migrate] {table}: {orphan} row(s) older than every batch, batch_id set to 0")
            conn.execute(text(f"UPDATE {table} SET batch_id = 0 WHERE batch_id IS NULL"))
        conn.execute(text(f"""
            UPDATE {table} c
            SET batch_age_days = c.stat_date - CAST(b.created_at AS date)
            FROM metric_batches b
            WHERE b.id = c.batch_id AND c.batch_age_days IS NULL
        """))

    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN batch_id SET NOT NULL"))

    pk_name, pk_cols = _primary_key(conn, table)
    if pk_cols != ["batch_id", "stat_date"]:
        if pk_name:
            conn.execute(text(f'ALTER TABLE {table} DROP CONSTRAINT "{pk_name}"'))
        conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {table}_pkey PRIMARY KEY (batch_id, stat_date)"))
        print(f"[migrate] {table}: primary key {pk_cols} -> ['batch_id', 'stat_date']")

    return first_time


def _clear_unmeasured_values(conn):
    """
    舊程式把「沒量過」寫成 0：
      - indexed_num / indexed_rate：接上 selectdb 之前一律是 0
      - ranked_num / ranked_rate：從來沒有實作，一律是 0
      - crawler_stat_*.indexed：寫死 0
    改成 NULL，圖上才會留空而不是畫出一條 0 的線。
    measured_at IS NULL 只會出現在這次升級之前寫入的列，新資料不受影響。
    """
    cutoff = conn.execute(text(
        "SELECT min(stat_date) FROM ("
        " SELECT stat_date FROM metric_headset_total WHERE indexed_num > 0"
        " UNION ALL"
        " SELECT stat_date FROM metric_randomset_total WHERE indexed_num > 0"
        ") t"
    )).scalar()

    for table in COVERAGE_TABLES:
        if cutoff is not None:
            n = conn.execute(text(f"""
                UPDATE {table}
                SET indexed_num = NULL, indexed_rate = NULL
                WHERE measured_at IS NULL AND indexed_num = 0 AND stat_date < :cutoff
            """), {"cutoff": cutoff}).rowcount
            if n:
                print(f"[migrate] {table}: indexed -> NULL on {n} row(s) before {cutoff}")
        n = conn.execute(text(f"""
            UPDATE {table}
            SET ranked_num = NULL, ranked_rate = NULL
            WHERE measured_at IS NULL AND ranked_num = 0
        """)).rowcount
        if n:
            print(f"[migrate] {table}: ranked -> NULL on {n} row(s)")


def _reclassify_legacy_recheck(conn, table):
    """
    舊列 (measured_at IS NULL，這次升級前寫入的) 依 measure.py 現在的規則重新判斷 is_recheck：
    batch 當天建立 (batch_age_days = 0) 為 t0，其他為重量；batch_age_days 是 NULL 的維持 false。
    只改值不同的列，所以可以重複執行，第二次起改動數為 0。
    """
    n = conn.execute(text(f"""
        UPDATE {table}
        SET is_recheck = COALESCE(batch_age_days <> 0, false)
        WHERE measured_at IS NULL
          AND is_recheck IS DISTINCT FROM COALESCE(batch_age_days <> 0, false)
    """)).rowcount
    print(f"[migrate] {table}: legacy is_recheck reclassified on {n} row(s)")


def migrate_metric_db(db):
    with db.engine.begin() as conn:
        if _table_exists(conn, "metric_url"):
            for col, col_type in METRIC_URL_COLUMNS:
                conn.execute(text(f"ALTER TABLE metric_url ADD COLUMN IF NOT EXISTS {col} {col_type}"))

        first_time = False
        for table in COVERAGE_TABLES:
            if _table_exists(conn, table):
                first_time = _migrate_coverage_table(conn, table) or first_time
                _reclassify_legacy_recheck(conn, table)

        if first_time:
            _clear_unmeasured_values(conn)
            # crawler_stat 沒有 measured_at 可以區分新舊資料，只在第一次升級時清一次
            for table in CRAWLER_STAT_TABLES:
                if _table_exists(conn, table):
                    n = conn.execute(text(f"UPDATE {table} SET indexed = NULL WHERE indexed = 0")).rowcount
                    if n:
                        print(f"[migrate] {table}: indexed -> NULL on {n} row(s)")
