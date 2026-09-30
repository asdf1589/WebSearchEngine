#!/usr/bin/env python3
"""Provision the metricdb connection, virtual datasets, charts, and dashboard
in a running Superset, via its REST API. Idempotent: safe to re-run.

Usage:
    python3 bootstrap.py            # talks to http://localhost:8088, admin/admin

Env overrides: SUPERSET_URL, SUPERSET_USER, SUPERSET_PASSWORD,
               METRIC_DB_HOST, METRIC_DB_PORT.
"""
import json
import os
import sys
import time

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ.get("SUPERSET_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

METRIC_DB_HOST = os.environ.get("METRIC_DB_HOST", "172.16.191.1")
METRIC_DB_PORT = os.environ.get("METRIC_DB_PORT", "5433")
METRIC_DB_URI = (
    f"postgresql+psycopg2://metric:metric@{METRIC_DB_HOST}:{METRIC_DB_PORT}/metricdb"
)

# --- virtual datasets: name -> SQL -------------------------------------------
# Coverage main lines only use the initial measurement of each batch
# (is_recheck = false), so later re-measurements don't interleave with it.
DATASETS = {
    "vw_crawler_overview": (
        "SELECT stat_date, discovered, crawled\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_index_selection": (
        "SELECT stat_date, indexed AS selected\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_crawled_rolling": (
        "SELECT stat_date,\n"
        "       fetch_ok    AS crawled_daily,\n"
        "       fetch_ok_7  AS crawled_weekly,\n"
        "       fetch_ok_30 AS crawled_monthly\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_request_effectiveness": (
        "SELECT stat_date,\n"
        "       ROUND(fetch_ok::numeric / NULLIF(fetch_total, 0), 4) AS eff_daily\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_headset_coverage": (
        "SELECT stat_date, discovered_rate, crawled_rate, indexed_rate\n"
        "FROM metric_headset_total WHERE NOT is_recheck ORDER BY stat_date"
    ),
    "vw_randomset_coverage": (
        "SELECT stat_date, discovered_rate, crawled_rate, indexed_rate\n"
        "FROM metric_randomset_total WHERE NOT is_recheck ORDER BY stat_date"
    ),
    # One line per batch age: how much of each batch was discovered on the day
    # it was built and 7 / 14 / 27 days later (before golden_inject at 4 weeks).
    "vw_headset_coverage_by_age": (
        "SELECT CAST(b.created_at AS date) AS batch_date,\n"
        "       'day ' || lpad(c.batch_age_days::text, 2, '0') AS measured,\n"
        "       c.discovered_rate, c.crawled_rate\n"
        "FROM metric_headset_total c JOIN metric_batches b ON b.id = c.batch_id\n"
        "WHERE c.batch_age_days IN (0, 7, 14, 27) ORDER BY batch_date"
    ),
    "vw_randomset_coverage_by_age": (
        "SELECT CAST(b.created_at AS date) AS batch_date,\n"
        "       'day ' || lpad(c.batch_age_days::text, 2, '0') AS measured,\n"
        "       c.discovered_rate, c.crawled_rate\n"
        "FROM metric_randomset_total c JOIN metric_batches b ON b.id = c.batch_id\n"
        "WHERE c.batch_age_days IN (0, 7, 14, 27) ORDER BY batch_date"
    ),
    "vw_request_sent": (
        "SELECT stat_date, fetch_total, fetch_ok, fetch_fail\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
}

# Time column per dataset (default: stat_date).
DATASET_TIME_COLUMN = {
    "vw_headset_coverage_by_age": "batch_date",
    "vw_randomset_coverage_by_age": "batch_date",
}


def metric(sql_expr, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label,
        "hasCustomLabel": True,
    }


# --- charts: each maps to one dataset and a set of line metrics --------------
CHARTS = [
    {
        "name": "Total Overview Volumn",
        "dataset": "vw_crawler_overview",
        "metrics": [
            metric("MAX(discovered)", "T - Discovered"),
            metric("MAX(crawled)", "T - Crawled"),
        ],
    },
    {
        # Own chart: ~3e7 selected URLs vanish at the bottom of a ~5e9 axis.
        "name": "Index Selection - Selected URLs",
        "markers": True,
        "dataset": "vw_index_selection",
        "metrics": [
            metric("MAX(selected)", "Selected"),
        ],
    },
    {
        "name": "Crawled (Daily / Weekly / Monthly)",
        "dataset": "vw_crawled_rolling",
        "metrics": [
            metric("MAX(crawled_daily)", "Crawled - Daily"),
            metric("MAX(crawled_weekly)", "Crawled - Weekly"),
            metric("MAX(crawled_monthly)", "Crawled - Monthly"),
        ],
    },
    {
        "name": "HeadSet Coverage",
        "markers": True,
        "dataset": "vw_headset_coverage",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "H - DiscCov"),
            metric("MAX(crawled_rate)", "H - CrawlCov"),
            metric("MAX(indexed_rate)", "H - IndexCov"),
        ],
    },
    {
        "name": "RandomSet Coverage",
        "markers": True,
        "dataset": "vw_randomset_coverage",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "R - DiscCov"),
            metric("MAX(crawled_rate)", "R - CrawlCov"),
            metric("MAX(indexed_rate)", "R - IndexCov"),
        ],
    },
    {
        "name": "HeadSet Discovery by Batch Age",
        "markers": True,
        "dataset": "vw_headset_coverage_by_age",
        "x_axis": "batch_date",
        "groupby": ["measured"],
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "H - DiscCov"),
        ],
    },
    {
        "name": "RandomSet Discovery by Batch Age",
        "markers": True,
        "dataset": "vw_randomset_coverage_by_age",
        "x_axis": "batch_date",
        "groupby": ["measured"],
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "R - DiscCov"),
        ],
    },
    {
        "name": "Request Effectiveness (Daily)",
        "dataset": "vw_request_effectiveness",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(eff_daily)", "Effectiveness - Daily"),
        ],
    },
    {
        "name": "Request Sent",
        "dataset": "vw_request_sent",
        "metrics": [
            metric("MAX(fetch_total)", "Fetch Total"),
            metric("MAX(fetch_ok)", "Fetch OK"),
            metric("MAX(fetch_fail)", "Fetch Fail"),
        ],
    },
]

DASHBOARD_TITLE = "Crawler Metrics"

# Loosen the cramped echarts rich-tooltip (tight <table> rows by default).
DASHBOARD_CSS = (
    ".echarts-tooltip table { border-collapse: separate !important;"
    " border-spacing: 0 5px !important; }\n"
    ".echarts-tooltip td { padding: 2px 8px !important; line-height: 1.5 !important; }\n"
    ".echarts-tooltip .x-value { line-height: 2.2 !important; }\n"
)


class Superset:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["Referer"] = BASE

    def login(self):
        r = self.s.post(
            f"{BASE}/api/v1/security/login",
            json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True},
        )
        r.raise_for_status()
        self.s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        r = self.s.get(f"{BASE}/api/v1/security/csrf_token/")
        r.raise_for_status()
        self.s.headers["X-CSRFToken"] = r.json()["result"]

    def _list(self, resource, name_col):
        r = self.s.get(f"{BASE}/api/v1/{resource}/?q=(page_size:100)")
        r.raise_for_status()
        return {row[name_col]: row["id"] for row in r.json()["result"]}

    def post(self, resource, payload):
        r = self.s.post(f"{BASE}/api/v1/{resource}/", json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"POST {resource} -> {r.status_code}: {r.text}")
        return r.json()["id"]

    # -- database -----------------------------------------------------------
    def ensure_database(self):
        existing = self._list("database", "database_name")
        if "metricdb" in existing:
            return existing["metricdb"]
        return self.post(
            "database",
            {
                "database_name": "metricdb",
                "sqlalchemy_uri": METRIC_DB_URI,
                "expose_in_sqllab": True,
            },
        )

    # -- datasets -----------------------------------------------------------
    def ensure_dataset(self, db_id, table_name, sql, time_col="stat_date"):
        existing = self._list("dataset", "table_name")
        if table_name in existing:
            ds_id = existing[table_name]
            r = self.s.put(f"{BASE}/api/v1/dataset/{ds_id}", json={"sql": sql})
            r.raise_for_status()
        else:
            ds_id = self.post(
                "dataset",
                {
                    "database": db_id,
                    "schema": "public",
                    "table_name": table_name,
                    "sql": sql,
                },
            )
        self._mark_temporal(ds_id, time_col)
        return ds_id

    def _mark_temporal(self, ds_id, col_name):
        r = self.s.get(f"{BASE}/api/v1/dataset/{ds_id}")
        r.raise_for_status()
        cols = r.json()["result"]["columns"]
        if not cols:
            return  # columns not inferred yet; chart still renders by name
        keep = ("id", "column_name", "type", "expression", "verbose_name",
                "groupby", "filterable", "description")
        payload_cols, changed = [], False
        for c in cols:
            d = {k: c.get(k) for k in keep if c.get(k) is not None}
            d["is_dttm"] = c["column_name"] == col_name
            if d["is_dttm"] != bool(c.get("is_dttm")):
                changed = True
            payload_cols.append(d)
        if changed:
            r = self.s.put(
                f"{BASE}/api/v1/dataset/{ds_id}?override_columns=true",
                json={"columns": payload_cols},
            )
            r.raise_for_status()

    # -- charts -------------------------------------------------------------
    def ensure_chart(self, spec, ds_id):
        existing = self._list("chart", "slice_name")
        params = {
            "viz_type": "echarts_timeseries_line",
            "datasource": f"{ds_id}__table",
            "x_axis": spec.get("x_axis", "stat_date"),
            "x_axis_sort_asc": True,
            "x_axis_title_margin": 15,
            "metrics": spec["metrics"],
            "groupby": spec.get("groupby", []),
            "adhoc_filters": [],
            "row_limit": 10000,
            "order_desc": True,
            "show_legend": True,
            # Sparse series (two batches a month, one point per re-measure age)
            # are invisible as a bare line until they have two points.
            "markerEnabled": spec.get("markers", False),
            "markerSize": 6,
            "opacity": 0.2,
            "seriesType": "line",
            "y_axis_format": spec.get("y_format", "SMART_NUMBER"),
            "rich_tooltip": True,
            # no summed "Total" row in the hover tooltip
            "showTooltipTotal": False,
            "showTooltipPercentage": False,
        }
        payload = {
            "slice_name": spec["name"],
            "viz_type": "echarts_timeseries_line",
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        if spec["name"] in existing:
            cid = existing[spec["name"]]
            r = self.s.put(f"{BASE}/api/v1/chart/{cid}", json=payload)
            r.raise_for_status()
            return cid
        return self.post("chart", payload)

    # -- dashboard ----------------------------------------------------------
    def ensure_dashboard(self, title, chart_ids_names):
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []},
            "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
        }
        # two charts per row (width 6 of the 12-column grid)
        for i, (cid, name) in enumerate(chart_ids_names):
            row_idx, col = i // 2, i % 2
            row_id, chart_node = f"ROW-{row_idx}", f"CHART-{i}"
            if col == 0:
                position["GRID_ID"]["children"].append(row_id)
                position[row_id] = {
                    "type": "ROW", "id": row_id, "parents": ["ROOT_ID", "GRID_ID"],
                    "children": [], "meta": {"background": "BACKGROUND_TRANSPARENT"},
                }
            position[row_id]["children"].append(chart_node)
            position[chart_node] = {
                "type": "CHART", "id": chart_node,
                "parents": ["ROOT_ID", "GRID_ID", row_id], "children": [],
                "meta": {"chartId": cid, "width": 6, "height": 50, "sliceName": name},
            }
        existing = self._list("dashboard", "dashboard_title")
        payload = {
            "dashboard_title": title,
            "position_json": json.dumps(position),
            "css": DASHBOARD_CSS,
            "published": True,
        }
        if title in existing:
            did = existing[title]
            r = self.s.put(f"{BASE}/api/v1/dashboard/{did}", json=payload)
            r.raise_for_status()
        else:
            did = self.post("dashboard", payload)
        # position_json references the charts but does not create the
        # slice<->dashboard association; link each chart explicitly.
        for cid, _ in chart_ids_names:
            r = self.s.put(f"{BASE}/api/v1/chart/{cid}", json={"dashboards": [did]})
            r.raise_for_status()
        return did


def wait_ready():
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/health", timeout=3).text.strip() == "OK":
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit("Superset did not become healthy in time")


def main():
    wait_ready()
    sup = Superset()
    sup.login()

    db_id = sup.ensure_database()
    print(f"database metricdb -> id {db_id}")

    ds_ids = {}
    for name, sql in DATASETS.items():
        ds_ids[name] = sup.ensure_dataset(
            db_id, name, sql, DATASET_TIME_COLUMN.get(name, "stat_date")
        )
        print(f"dataset {name} -> id {ds_ids[name]}")

    chart_ids = []
    for spec in CHARTS:
        cid = sup.ensure_chart(spec, ds_ids[spec["dataset"]])
        chart_ids.append((cid, spec["name"]))
        print(f"chart {spec['name']!r} -> id {cid}")

    did = sup.ensure_dashboard(DASHBOARD_TITLE, chart_ids)
    print(f"dashboard {DASHBOARD_TITLE!r} -> id {did}")
    print(f"\nDone. Open {BASE}/superset/dashboard/{did}/")


if __name__ == "__main__":
    main()
