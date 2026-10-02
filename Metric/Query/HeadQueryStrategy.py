from Metric.Query.QueryStrategy import QueryStrategy
from sqlalchemy import func

class HeadQueryStrategy(QueryStrategy):
    def __init__(self, db, modelFactory, batch_id, rawData, keywordNums):
        """
        :param db: Database instance
        :param modelFactory: AppModelFactory instance
        :param batch_id: 當前執行的 MetricBatch ID
        :param rawData: 原始資料列表 (通常來自 RawDataReader.readData())
        :param keywordNums: 要選取前幾名
        """
        super().__init__(rawData, keywordNums)
        self.db = db
        self.modelFactory = modelFactory
        self.batch_id = batch_id
    
    def getGoldenSet(self):
        # 根據 Frequency 由高到低往下挑，跳過已帶 head tag 的關鍵字
        self._collectGoldenSet(
            "head",
            lambda candidates, need: sorted(candidates, key=lambda x: x["frequency"], reverse=True)[:need]
        )

    def _update_batch_stats(self, session, MetricBatch, MetricQuery, MetricURL):
        """
        輔助函式：重新計算並更新 Batch 的 Metadata
        """
        batch = session.get(MetricBatch, self.batch_id)
        if not batch:
            return

        # 1. 計算 Head Set 的統計數據
        # Head Queries 數量
        head_q_count = session.query(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["head"])
        ).count()

        # Head URLs 數量 (透過 Join 計算)
        head_u_count = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["head"])
        ).count()

        # 2. 更新 meta_tag_stats
        # 先取出舊的 dict (如果有的話)
        current_stats = dict(batch.meta_tag_stats) if batch.meta_tag_stats else {}
        current_stats['head'] = {
            "queries": head_q_count,
            "urls": head_u_count
        }
        batch.meta_tag_stats = current_stats

        # 3. 更新全域總量 (Total Stats)
        # 這裡直接重算整個 Batch 的總量，確保數據一致性
        total_q = session.query(MetricQuery).filter_by(batch_id=self.batch_id).count()
        total_u = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id
        ).count()

        batch.meta_total_queries = total_q
        batch.meta_total_urls = total_u

        session.commit()
        print(f"Batch {self.batch_id} Stats Updated: Head Queries={head_q_count}, Head URLs={head_u_count}")