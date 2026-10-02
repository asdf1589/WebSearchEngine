from serpapi import GoogleSearch
from tqdm import tqdm
import time
import os

class QueryStrategy:
    def __init__(self, rawData: list, keywordNums: int):
        self.rawData: list = rawData
        self.keywordNums: int = keywordNums

    def getGoldenSet(self):
        pass

    def _collectGoldenSet(self, tag, pickNew):
        """
        收集這個 batch 某個 tag 的 golden set，重跑 --create 時不改動已收集的部分。
        子類別需設定 self.db、self.modelFactory、self.batch_id，並提供 _update_batch_stats。

        - 帶這個 tag 且至少有一條 metric_url 的 query 達到 keywordNums：整個跳過，不呼叫 SerpApi。
        - 不足時補齊：帶這個 tag 但沒有 URL 的 query 重新查詢；帶 tag 的 query 仍少於 keywordNums，
          由 pickNew(未帶這個 tag 的關鍵字, 需要的數量) 挑新的關鍵字補上。
        - 關鍵字已經有 URL (被另一個 tag 挑過)：只補 tag，不重新查詢。
        - 不刪除任何既有的 metric_url。
        - 最後一律重算 batch 的統計數據。

        :param tag: "head" 或 "random"
        :param pickNew: (candidates: list, need: int) -> list，從 rawData 的子集挑出新的關鍵字
        """
        MetricQuery = self.modelFactory.create_metric_queries()
        MetricURL = self.modelFactory.create_metric_url()
        MetricBatch = self.modelFactory.create_metric_batches()

        # rawData 少於 keywordNums 時，全部挑完就算完整
        target_num = min(len(self.rawData), self.keywordNums)

        with self.db.session() as session:
            tagged = session.query(MetricQuery).filter(
                MetricQuery.batch_id == self.batch_id,
                MetricQuery.tags.contains([tag])
            ).all()
            with_urls = {qid for (qid,) in session.query(MetricURL.query_id).filter(
                MetricURL.query_id.in_([q.id for q in tagged])
            ).distinct()}

            if len(with_urls) >= target_num:
                print(f"Batch {self.batch_id} tag '{tag}': {len(with_urls)} queries already have URLs "
                      f"(keywordNums {target_num}); golden set is complete, skip without calling SerpApi.")
            else:
                missing = [q for q in tagged if q.id not in with_urls]
                tagged_keywords = {q.keyword for q in tagged}
                candidates = [s for s in self.rawData if s['keyword'] not in tagged_keywords]
                new_data = pickNew(candidates, max(target_num - len(tagged), 0))
                print(f"Processing {tag} strategy for Batch {self.batch_id}: {len(with_urls)} queries already have URLs; "
                      f"re-querying {len(missing)} tagged queries without URLs, adding {len(new_data)} new keywords "
                      f"(keywordNums {target_num})...")
                pbar = tqdm(total=len(missing) + len(new_data))
                tag_only = 0

                for query_obj in missing:
                    self._writeUrls(session, MetricURL, query_obj)
                    # 每一筆 Commit 一次，避免長時間佔用 Transaction 或 API 中斷導致全部回滾
                    session.commit()
                    pbar.update(1)

                for s in new_data:
                    key = s['keyword']
                    # 先檢查這個關鍵字在這個 Batch 是否已經存在 (例如由 Trending 匯入過)
                    query_obj = session.query(MetricQuery).filter_by(
                        batch_id=self.batch_id,
                        keyword=key
                    ).first()

                    if query_obj:
                        current_tags = list(query_obj.tags) # 複製 list
                        current_tags.append(tag)
                        query_obj.tags = current_tags # 觸發 SQLAlchemy 更新
                    else:
                        query_obj = MetricQuery(
                            batch_id=self.batch_id,
                            keyword=key,
                            geo=s.get('geo', []), # 繼承原始資料的 geo
                            frequency=s['frequency'],
                            tags=[tag]
                        )
                        session.add(query_obj)
                        session.flush() # 取得 ID

                    # 已經有 URL (被另一個 tag 挑過) 就沿用，不重新查詢，先前量到的逐條結果才會保留
                    has_urls = session.query(MetricURL.id).filter_by(query_id=query_obj.id).first() is not None
                    if has_urls:
                        tag_only += 1
                    else:
                        self._writeUrls(session, MetricURL, query_obj)

                    session.commit()
                    pbar.update(1)

                pbar.close()
                print(f"Batch {self.batch_id} tag '{tag}': re-queried {len(missing)}, added {len(new_data)} new keywords "
                      f"({tag_only} already had URLs from another tag, tag added only)")

            self._update_batch_stats(session, MetricBatch, MetricQuery, MetricURL)

    def _writeUrls(self, session, MetricURL, query_obj):
        """呼叫 SerpApi 並寫入這個 query 的 URL (呼叫前這個 query 沒有任何 URL)"""
        # 注意：這裡會消耗 API 額度與時間
        url_list = self.getQuery(query_obj.keyword)
        for idx, u in enumerate(url_list):
            session.add(MetricURL(
                query_id=query_obj.id,
                url=u,
                rank=idx + 1
            ))
    
    def getQuery(self, query, nums=10, max_retries=3, initial_delay=15):
        """
        Args:
            max_retries (int): 最大重試次數
            initial_delay (int): 初始等待秒數 (會隨著重試次數增加)
        """
        for attempt in range(max_retries):
            try:
                params = {
                    "engine": "google",
                    "q": query,
                    "num": nums,
                    "api_key": os.environ.get('SERPAPI_KEY')
                }

                search = GoogleSearch(params)
                results = search.get_dict()
                
                # SerpApi 有時會回傳 200 OK 但內容包含 error 欄位
                if "error" in results:
                    raise Exception(f"SerpApi Error: {results['error']}")

                # 成功取得資料
                urls = [r["link"] for r in results.get("organic_results", []) if "link" in r]
                return urls

            except Exception as e:
                print(f"[Attempt {attempt + 1}/{max_retries}] Failed: {e}")
                
                # 如果還沒達到最大重試次數，就等待後重試
                if attempt < max_retries - 1:
                    # 指數退避: 2s, 4s, 8s...
                    wait_time = initial_delay * (2 ** attempt) 
                    print(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    # 最後一次也失敗，回傳空 list；錯誤訊息一併印出，才看得出是額度用完還是其他原因
                    print(f"Max retries reached for query '{query}', returning empty list. Last error: {e}")
                    return []