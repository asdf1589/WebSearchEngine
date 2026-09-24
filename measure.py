from Metric.RawDataReader.RawDataReader import RawDataReader
from Metric.RawDataReader.DatabaseRawDataReader import DatabaseRawDataReader


from Metric.Query.QueryContext import QueryContext
from Metric.Query.RandomQueryStrategy import RandomQueryStrategy
from Metric.Query.HeadQueryStrategy import HeadQueryStrategy

from Metric.Measure.MeasureContext import MeasureContext
from Metric.Measure.TypesenseRankMeasure import TypesenseRankMeasure
from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure
from Metric.Measure.SearchEngineAllMetricMeasure import SearchEngineAllMetricMeasure
from Metric.Measure.CrawlerStatusMeasure import CrawlerStatusMeasure

from Database.Database import Database
from Database.CrawlerModels import Base as CrawlerBase
from Database.MetricModels import Base as MetricBase
from Database.ModelFactory.AppModelFactory import AppModelFactory
from Database.utils import createAllMetricModel, createDB
from Database.migrations import migrate_metric_db

from argparse import ArgumentParser

import os
from datetime import datetime, timedelta

from sqlalchemy import select, desc

def get_latest_batch_id(db, modelFactory) -> int:
    """
    獲取最新 MetricBatch 的 ID。
    回傳: int (若表為空則回傳 None)
    """
    MetricBatch = modelFactory.create_metric_batches()
    
    with db.session() as session:
        # 1. 只選取 id 欄位 (Scalar Select) -> 節省記憶體與傳輸頻寬
        # 2. 根據 id 倒序排列 (利用 PK Index) -> 極速
        # 3. 取第一筆
        stmt = select(MetricBatch.id).order_by(MetricBatch.id.desc()).limit(1)
        
        # scalar() 會直接回傳數值 (int) 或 None
        latest_id = session.execute(stmt).scalar()
        
        return latest_id

def get_batch_ids_by_age(db, modelFactory, ages) -> list:
    """
    回傳「今天距建立日期剛好 N 天」(N 屬於 ages) 的 batch id，由舊到新。
    給每天跑一次的重量 cron 用：batch 建立後第 7、14、27 天各量一次。
    """
    MetricBatch = modelFactory.create_metric_batches()
    today = datetime.now().date()
    oldest = datetime.combine(today - timedelta(days=max(ages)), datetime.min.time())

    with db.session() as session:
        rows = session.execute(
            select(MetricBatch.id, MetricBatch.created_at)
            .where(MetricBatch.created_at >= oldest)
            .order_by(MetricBatch.id)
        ).all()

    return [bid for bid, created_at in rows if (today - created_at.date()).days in ages]

def parseArgs():
    parser = ArgumentParser()

    parser.add_argument("--strategy", nargs='+', choices=['random', 'head'], default=[], help="raw data path")
    parser.add_argument("--crawler_db_url", help="crawler url")
    parser.add_argument("--metric_db_url", help="metric url")
    parser.add_argument("--select_db_url", help="selectdb url (for index coverage)")

    parser.add_argument("--create", action='store_true', help="create dataset")
    parser.add_argument("--rawdatareader", choices=['db'], default='db', help="raw data reader strategy")
    parser.add_argument("--update", type=int, default=14, help="auto generator days")
    parser.add_argument("--keywordNums", type=int, default=100, help="Metric Data Keyword Nums")

    parser.add_argument("--test", action='store_true', help="test performance")
    parser.add_argument("--typesense_url", help="typesense url")
    parser.add_argument("--measure", nargs='+', choices=['status', 'rank', 'crawler_all', 'all'], help="raw data path")
    parser.add_argument("--batch_id", type=int, help="measure this batch instead of the latest one (crawler_all)")
    parser.add_argument("--batch_age_days", type=int, nargs='+', help="measure every batch created exactly N days ago (crawler_all)")

    parser.add_argument("--createtable", action='store_true', help="create table")

    args = parser.parse_args()
    return args


def createDataset(args, modelFactory: AppModelFactory, crawlerDB, metricDB):
    rawDataReader: RawDataReader = None
    if args.rawdatareader == "db":
        rawDataReader = DatabaseRawDataReader(metricDB, modelFactory, args.update)
    rawData = rawDataReader.readData()
    batch_id = get_latest_batch_id(metricDB, modelFactory)

    context: QueryContext = QueryContext()

    if 'random' in args.strategy:
        context.setQueryStrategy(RandomQueryStrategy(metricDB, modelFactory, batch_id, rawData, args.keywordNums))
        context.getGoldenSet()
    if 'head' in args.strategy:
        context.setQueryStrategy(HeadQueryStrategy(metricDB, modelFactory, batch_id, rawData, args.keywordNums))
        context.getGoldenSet()

def test(args, modelFactory: AppModelFactory, crawlerDB, metricDB, selectDB):
    context: MeasureContext = MeasureContext()

    if 'status' in args.measure:
        context.setMeasure(CrawlerStatusMeasure(modelFactory, crawlerDB, metricDB, selectDB))
        context.test()

    if 'crawler_all' in args.measure:
        if args.batch_age_days:
            batch_ids = get_batch_ids_by_age(metricDB, modelFactory, args.batch_age_days)
            if not batch_ids:
                print(f"No batch is exactly {args.batch_age_days} day(s) old today; nothing to re-measure.")
        elif args.batch_id is not None:
            batch_ids = [args.batch_id]
        else:
            batch_ids = [get_latest_batch_id(metricDB, modelFactory)]

        # 只有跟 --create 一起跑、剛建好 golden set 的那次量測算 t0，其他都是事後重量
        is_recheck = not args.create

        for batch_id in batch_ids:
            for tag in args.strategy:
                context.setMeasure(CrawlerAllMetricMeasure(modelFactory, crawlerDB, metricDB, selectDB, batch_id, tag, is_recheck))
                context.test()

def main():
    args = parseArgs()

    modelFactory = AppModelFactory(CrawlerBase, MetricBase)
    
    if args.createtable:
        createAllMetricModel(modelFactory)
    
    crawlerDB = createDB("crawler", "crawler", args.crawler_db_url, "crawlerdb")
    metricDB = createDB("metric", "metric", args.metric_db_url, "metricdb", args.createtable, MetricBase)
    if args.createtable:
        # create_all 不會改已存在的表，舊表的新欄位與主鍵由這裡補上
        migrate_metric_db(metricDB)
    selectDB = createDB("select", "select", args.select_db_url, "selectdb") if args.select_db_url else None

    if args.create:
        createDataset(args, modelFactory, crawlerDB, metricDB)
    if args.test:
        test(args, modelFactory, crawlerDB, metricDB, selectDB)

if __name__ == '__main__':
    main()
