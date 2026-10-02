from Metric.Query.QueryStrategy import QueryStrategy
import random
from sqlalchemy import func

class RandomQueryStrategy(QueryStrategy):
    def __init__(self, db, modelFactory, batch_id, rawData, keywordNums):
        """
        :param db: Database instance
        :param modelFactory: AppModelFactory instance
        :param batch_id: 當前執行的 MetricBatch ID
        :param rawData: 原始資料列表
        :param keywordNums: 要隨機選取的數量
        """
        # dataset 傳 None
        super().__init__(rawData, keywordNums)
        self.db = db
        self.modelFactory = modelFactory
        self.batch_id = batch_id
    
    def getGoldenSet(self):
        # 從還沒帶 random tag 的關鍵字裡隨機抽樣
        # 注意：如果候選數量小於需要的數量，sample 會報錯，需做防呆
        self._collectGoldenSet(
            "random",
            lambda candidates, need: random.sample(candidates, min(len(candidates), need))
        )

    def _update_batch_stats(self, session, MetricBatch, MetricQuery, MetricURL):
        """
        重新計算並更新 Batch 的 Metadata
        """
        batch = session.get(MetricBatch, self.batch_id)
        if not batch:
            return

        # 1. 計算 Random Set 的統計數據
        random_q_count = session.query(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["random"])
        ).count()

        random_u_count = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id,
            MetricQuery.tags.contains(["random"])
        ).count()

        # 2. 更新 meta_tag_stats
        # 必須先讀取現有的 dict，再更新 random 的部分，以免覆蓋掉 head 的數據
        current_stats = dict(batch.meta_tag_stats) if batch.meta_tag_stats else {}
        
        current_stats['random'] = {
            "queries": random_q_count,
            "urls": random_u_count
        }
        batch.meta_tag_stats = current_stats

        # 3. 更新全域總量 (Total Stats)
        total_q = session.query(MetricQuery).filter_by(batch_id=self.batch_id).count()
        total_u = session.query(MetricURL).join(MetricQuery).filter(
            MetricQuery.batch_id == self.batch_id
        ).count()

        batch.meta_total_queries = total_q
        batch.meta_total_urls = total_u

        session.commit()
        print(f"Batch {self.batch_id} Stats Updated: Random Queries={random_q_count}, Random URLs={random_u_count}")