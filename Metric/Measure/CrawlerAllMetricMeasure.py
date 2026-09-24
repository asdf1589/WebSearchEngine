from Metric.Measure.Measure import Measure
from Database.Database import Database
from Database.ModelFactory.AppModelFactory import AppModelFactory
from sqlalchemy import select, and_, text
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
import tldextract
from urllib.parse import urlparse
from w3lib.url import canonicalize_url
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def to_canonical(url: str) -> str:
    """
    與 crawler spider 相同的正規化 (w3lib canonicalize_url)。
    crawlerdb 的 url_state_current 與 selectdb 的 selected_urls_current 存的都是這個寫法，
    golden URL 要先轉成同樣寫法才能用字串比對。
    """
    try:
        return canonicalize_url(url)
    except Exception:
        return url


# 從 url_state_current 帶回 metric_url 的欄位 (用來分析未抓到的原因)
DETAIL_KEYS = (
    "first_seen",
    "last_scheduled",
    "source",
    "robots_bits",
    "last_fail_reason",
    "num_scheduled_90d",
    "num_fetch_fail_90d",
)


class CrawlerAllMetricMeasure(Measure):
    def __init__(self, modelFactory: AppModelFactory, crawlerDB: Database, metricDB: Database, selectDB: Database, batch_id: int, tag: str, is_recheck: bool = False):
        """
        初始化 CrawlerAllMetricMeasure
        :param modelFactory: 模型工廠
        :param crawlerDB: 爬蟲資料庫 (讀取 Shard 狀態)
        :param metricDB: 指標資料庫 (讀取 Golden URL / 寫入覆蓋率)
        :param selectDB: 選址資料庫 (查詢 selected_urls_current)
        :param batch_id: 指定要評估的 MetricBatch ID
        :param tag: 指定 Metric 標籤 (例如 'head', 'random')，用來篩選 Golden URLs
        :param is_recheck: False 表示這是建立 golden set 當下的量測 (t0)，True 表示事後重量
        """
        super().__init__()
        self.modelFactory = modelFactory
        self.crawlerDB: Database = crawlerDB
        self.metricDB: Database = metricDB
        self.selectDB: Database = selectDB
        self.batch_id = batch_id
        self.tag = tag
        self.is_recheck = is_recheck
        
        # 初始化 TLD Extractor (關閉快取避免權限問題)
        self.extractor = tldextract.TLDExtract(cache_dir=False)
    
    def get_domain(self, url: str) -> str:
        try:
            extracted = self.extractor(url)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return ""
        except:
            return ""

    def get_host(self, url: str) -> str:
        try:
            return (urlparse(url).hostname or "").lower()
        except Exception:
            return ""

    def _scan_domain_shard(self, domain_tuple):
        """
        掃描 CrawlerDB 的 Domain Tables，建立 Domain -> Shard ID 的映射
        """
        if not domain_tuple:
            return {}

        found_map = {}
        with self.crawlerDB.session() as session:
            try:
                DomainState = self.modelFactory.create_domain_state_model()
                stmt = (
                    select(DomainState.domain, DomainState.shard_id)
                    .where(DomainState.domain.in_(domain_tuple))
                )

                for domain, shard_id in session.execute(stmt).all():
                    found_map[domain] = shard_id

            except Exception as e:
                print(f"[Error] scan domain_state failed: {e}")

        return found_map

    def _load_indexed_urls(self, url_tuple):
        """
        查詢 SelectDB 的 selected_urls_current，回傳有被選取的 URL 集合
        """
        if not url_tuple:
            return set()

        indexed_set = set()
        chunk_size = 10000

        with self.selectDB.session() as session:
            for i in range(0, len(url_tuple), chunk_size):
                chunk = url_tuple[i:i + chunk_size]
                stmt = text(
                    "SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)"
                )
                rows = session.execute(stmt, {"urls": list(chunk)}).fetchall()
                for row in rows:
                    indexed_set.add(row[0])

        return indexed_set

    def _scan_url_shard(self, shard_ids, url_tuple):
        """
        掃描 CrawlerDB 的 Url State Tables，取得 URL 的發現/爬取狀態
        回傳: (List[dict], List[failed shard_id])
        """
        found_data = []
        failed_shards = []
        
        with self.crawlerDB.session() as session:
            for i in shard_ids:
                try:
                    UrlState = self.modelFactory.create_url_state_current_model(i)
                    # 判斷 URL 是否存在於該分片
                    stmt = select(
                        UrlState.url,
                        UrlState.last_fetch_ok.is_not(None).label("crawled"),
                        UrlState.first_seen,
                        UrlState.last_scheduled,
                        UrlState.source,
                        UrlState.robots_bits,
                        UrlState.last_fail_reason,
                        UrlState.num_scheduled_90d,
                        UrlState.num_fetch_fail_90d,
                    ).where(UrlState.url.in_(url_tuple))

                    for row in session.execute(stmt).all():
                        rec = dict(row._mapping)
                        rec["crawled"] = bool(rec["crawled"])
                        rec["shard_id"] = i
                        found_data.append(rec)
                except Exception as e:
                    # 失敗後 transaction 會停在 aborted 狀態，不 rollback 的話後面的 shard 也會全部失敗
                    session.rollback()
                    failed_shards.append(i)
                    print(f"[Error] scan url_state_current_{i:03d} failed: {e}")
        return found_data, failed_shards

    def _get_batch_age_days(self):
        MetricBatch = self.modelFactory.create_metric_batches()
        with self.metricDB.session() as session:
            created_at = session.execute(
                select(MetricBatch.created_at).where(MetricBatch.id == self.batch_id)
            ).scalar()
        if created_at is None:
            return None
        return (datetime.now().date() - created_at.date()).days

    def test(self):
        """
        主執行邏輯 (使用 __init__ 傳入的 batch_id 和 tag)
        """
        print(f'🚀 Start Measuring Crawler Coverage (Batch: {self.batch_id}, Tag: {self.tag}, Recheck: {self.is_recheck})')
        measured_at = datetime.now()
        today_date = measured_at.date()
        batch_age_days = self._get_batch_age_days()
        
        # 對應 MetricCoverage 的 Set Type (例如 "head" -> "HeadSet")
        set_type = f"{self.tag.capitalize()}Set"

        # ==========================================
        # 1. 從 MetricDB 讀取 Golden URLs
        # ==========================================
        MetricURL = self.modelFactory.create_metric_url()
        MetricQuery = self.modelFactory.create_metric_queries()
        
        # 結構: 正規化後的 url -> list of MetricURL ID
        url_id_map = {} 
        # 查詢用的字串 -> 正規化後的 url。
        # 同時查原始寫法，是為了找到修正前 golden_inject 用原始字串插入 crawlerdb 的舊資料列。
        lookup_to_canonical = {}
        all_golden_domains = set()
        changed_by_canonical = 0
        
        print(f"📥 Loading Golden URLs with tag '{self.tag}'...")
        with self.metricDB.session() as session:
            # 透過 Join 篩選：
            # 1. MetricQuery.batch_id 符合
            # 2. MetricQuery.tags 包含指定的 tag (使用 JSONB @> 操作符)
            stmt = select(MetricURL)\
                .join(MetricQuery)\
                .where(
                    and_(
                        MetricQuery.batch_id == self.batch_id,
                        MetricQuery.tags.contains([self.tag])
                    )
                )
            
            
            results = session.execute(stmt).scalars().all()
            
            if not results:
                print(f"⚠️ No URLs found for Batch {self.batch_id} with tag '{self.tag}'. Exiting.")
                return

            for m_url in results:
                raw_url = m_url.url
                url_str = to_canonical(raw_url)
                if url_str != raw_url:
                    changed_by_canonical += 1
                
                if url_str not in url_id_map:
                    url_id_map[url_str] = []
                
                url_id_map[url_str].append(m_url.id)
                lookup_to_canonical[url_str] = url_str
                lookup_to_canonical.setdefault(raw_url, url_str)

                # domain_state.domain 對一般 domain 存 eTLD+1，對被拆出來的 subdomain 存完整 host，兩種都查
                for domain in (self.get_host(url_str), self.get_domain(url_str)):
                    if domain:
                        all_golden_domains.add(domain)

        # 準備掃描用的 Tuple
        url_list = list(url_id_map.keys())
        lookup_tuple = tuple(lookup_to_canonical.keys())
        domain_tuple = tuple(all_golden_domains)
        
        print(f"   Loaded {len(results)} rows -> {len(url_list)} unique URLs from {len(domain_tuple)} hosts/domains.")
        print(f"   {changed_by_canonical} rows change under canonicalize_url.")

        # ==========================================
        # 2. 並行掃描 CrawlerDB (Shards)
        # ==========================================
        MAX_WORKERS = 16
        chunk_size = 256 // MAX_WORKERS + 1
        shard_chunks = [range(i, min(i + chunk_size, 256)) for i in range(0, 256, chunk_size)]

        # A. 掃描 Domain Tables (為了確定 Shard ID / Team)
        print(f"🔍 Scanning Domain Tables ...")
        domain_shard_map = self._scan_domain_shard(domain_tuple)

        # B. 掃描 URL Tables (為了確定 Status)
        # 初始化 URL 狀態
        url_status_map = {
            u: {'discovered': False, 'crawled': False, 'indexed': False, 'shard_id': -1, **{k: None for k in DETAIL_KEYS}}
            for u in url_list
        }
        
        failed_shards = []
        print(f"🔍 Scanning URL Tables ({MAX_WORKERS} threads)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._scan_url_shard, chunk, lookup_tuple) for chunk in shard_chunks]
            for future in tqdm(as_completed(futures), total=len(futures), desc="URLs"):
                rows, failed = future.result()
                failed_shards.extend(failed)
                for row in rows:
                    url = lookup_to_canonical.get(row['url'])
                    if url is None:
                        continue
                    s = url_status_map[url]
                    first_hit = not s['discovered']
                    earliest = min(
                        (t for t in (s['first_seen'], row['first_seen']) if t is not None),
                        default=None,
                    )
                    s['discovered'] = True
                    # 同一條 URL 可能在多個 shard 各有一列 (reshard / subdomain 拆分之後)，
                    # 也可能同時有原始寫法與正規化寫法兩列：任一列抓過就算 crawled，
                    # 其餘欄位以抓過的那一列為準，不能讓後掃到的列蓋掉。
                    if first_hit or (row['crawled'] and not s['crawled']):
                        s['shard_id'] = row['shard_id']
                        for k in DETAIL_KEYS:
                            s[k] = row[k]
                    s['crawled'] = s['crawled'] or row['crawled']
                    s['first_seen'] = earliest

        if failed_shards:
            # 少掃到的 shard 會讓 discovered / crawled 偏低，寫進報表會被當成真的下降，所以直接中止
            raise RuntimeError(
                f"url_state_current scan failed on {len(failed_shards)} shard(s): "
                f"{sorted(failed_shards)[:20]}; coverage not written"
            )

        # 查詢 SelectDB 取得 indexed 狀態。沒有 SelectDB 或查詢失敗時 indexed 記為 NULL (沒量到)，而不是 0。
        indexed_measured = False
        if self.selectDB is not None:
            print(f"🔍 Checking SelectDB for index status ...")
            try:
                indexed_urls = self._load_indexed_urls(lookup_tuple)
                for url in indexed_urls:
                    canon = lookup_to_canonical.get(url)
                    if canon is not None:
                        url_status_map[canon]['indexed'] = True
                indexed_measured = True
            except Exception as e:
                print(f"[Warning] selectdb lookup failed, indexed coverage left empty: {e}")

        # ==========================================
        # 3. 聚合統計與分組 (Team A / Team B)
        # ==========================================
        print("🔄 Aggregating Stats...")
        
        # 統計容器：分別統計 Total, Team A, Team B
        stats = {
            "Total": {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
            "A":     {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
            "B":     {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
        }
        
        bulk_update_mappings = []

        for url_str, ids in url_id_map.items():
            status = url_status_map.get(url_str, {})
            
            is_disc = status.get('discovered', False)
            is_crawl = status.get('crawled', False)
            is_idx = status.get('indexed', False)
            shard_id = status.get('shard_id', -1)
            
            # 如果 URL table 沒找到，嘗試用 Domain table 找 Team
            # 被拆出來的 subdomain 在 domain_state 以完整 host 存，先查 host 再查 eTLD+1
            if shard_id == -1:
                shard_id = domain_shard_map.get(self.get_host(url_str), -1)
            if shard_id == -1:
                shard_id = domain_shard_map.get(self.get_domain(url_str), -1)

            # 判斷 Team
            team_key = None
            if 0 <= shard_id <= 127:
                team_key = "A"
            elif 128 <= shard_id <= 255:
                team_key = "B"
            
            # 準備批量更新 MetricURL 的資料
            for m_id in ids:
                bulk_update_mappings.append({
                    "id": m_id,
                    "url_canonical": url_str,
                    "is_discovered": is_disc,
                    "is_crawled": is_crawl,
                    "is_indexed": is_idx if indexed_measured else None,
                    "shard_id": shard_id, # 記錄找到的 shard，-1 表示未找到
                    **{k: status.get(k) for k in DETAIL_KEYS},
                })

            # 累加統計 (Team A/B 與 Total)
            target_groups = ["Total"]
            if team_key:
                target_groups.append(team_key)
            
            for g in target_groups:
                stats[g]["total"] += 1
                if is_disc: stats[g]["disc"] += 1
                if is_crawl: stats[g]["crawl"] += 1
                if is_idx:  stats[g]["idx"] += 1

        # ==========================================
        # 4. 寫入 MetricDB
        # ==========================================
        print(f"💾 Saving results to MetricDB...")
        
        with self.metricDB.session() as session:
            # A. 更新 MetricURL 詳細狀態
            if not self.is_recheck:
                if bulk_update_mappings:
                    session.bulk_update_mappings(MetricURL, bulk_update_mappings)
            else:
                # 重量不覆蓋 metric_url 上 t0 的狀態，只補 url_canonical；逐條結果寫進 metric_url_recheck
                session.bulk_update_mappings(
                    MetricURL,
                    [{"id": m["id"], "url_canonical": m["url_canonical"]} for m in bulk_update_mappings],
                )
                self._save_recheck_rows(session, bulk_update_mappings, today_date, measured_at, batch_age_days)
            
            # B. 寫入 MetricCoverage 統計表 (Total, A, B)
            suffixes = ["Total", "A", "B"]
            
            for suffix in suffixes:
                d = stats[suffix]
                total_count = d["total"]
                
                try:
                    ModelClass = self.modelFactory.create_metric_coverage_model(set_type, suffix)
                except Exception as e:
                    print(f"   [Warning] Could not create model for {set_type}_{suffix}: {e}")
                    continue

                row_data = {
                    "batch_id": self.batch_id,
                    "stat_date": today_date,
                    "measured_at": measured_at,
                    "batch_age_days": batch_age_days,
                    "is_recheck": self.is_recheck,
                    "total": total_count,
                    "discovered_num": d["disc"],
                    "discovered_rate": d["disc"] / total_count if total_count > 0 else 0.0,
                    "crawled_num": d["crawl"],
                    "crawled_rate": d["crawl"] / total_count if total_count > 0 else 0.0,
                    # 沒量到 (沒有 selectdb 或查詢失敗) 寫 NULL，圖上會留空，不會畫成 0
                    "indexed_num": d["idx"] if indexed_measured else None,
                    "indexed_rate": (d["idx"] / total_count if total_count > 0 else 0.0) if indexed_measured else None,
                    # 排名覆蓋率尚未實作
                    "ranked_num": None,
                    "ranked_rate": None,
                }

                stmt = insert(ModelClass).values(row_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['batch_id', 'stat_date'],
                    set_=row_data
                )
                session.execute(stmt)
            
            session.commit()
            print("✅ MetricDB Updated Successfully.")

        # ==========================================
        # 5. 輸出報告
        # ==========================================
        self._print_report(stats, indexed_measured, batch_age_days)

    def _save_recheck_rows(self, session, mappings, stat_date, measured_at, batch_age_days):
        MetricURLRecheck = self.modelFactory.create_metric_url_recheck()
        label_keys = ("is_discovered", "is_crawled", "is_indexed", "shard_id") + DETAIL_KEYS
        rows = [
            {
                "metric_url_id": m["id"],
                "stat_date": stat_date,
                "batch_age_days": batch_age_days,
                "measured_at": measured_at,
                **{k: m[k] for k in label_keys},
            }
            for m in mappings
        ]
        chunk_size = 2000
        for i in range(0, len(rows), chunk_size):
            stmt = insert(MetricURLRecheck).values(rows[i:i + chunk_size])
            stmt = stmt.on_conflict_do_update(
                index_elements=['metric_url_id', 'stat_date'],
                set_={k: stmt.excluded[k] for k in ("batch_age_days", "measured_at") + label_keys},
            )
            session.execute(stmt)

    def _print_report(self, stats, indexed_measured=True, batch_age_days=None):
        print("\n" + "="*60)
        print(f"📊 Coverage Report - Tag: {self.tag}, Batch: {self.batch_id}, Age: {batch_age_days} day(s)")
        print("="*60)
        
        headers = f"{'Group':<12} | {'Total':>8} | {'Disc %':>10} | {'Crawl %':>10} | {'Index %':>10}"
        print(headers)
        print("-" * len(headers))
        
        groups = ["Total", "A", "B"]
        
        for g in groups:
            d = stats[g]
            total = d['total']
            if total == 0:
                print(f"{g:<12} | {0:>8} | {'N/A':>10} | {'N/A':>10} | {'N/A':>10}")
                continue
                
            disc_rate = d['disc'] / total
            crawl_rate = d['crawl'] / total
            idx_text = f"{d['idx'] / total:>9.1%}" if indexed_measured else f"{'N/A':>10}"
            
            print(f"{g:<12} | {total:>8,} | {disc_rate:>9.1%} | {crawl_rate:>9.1%} | {idx_text}")
        
        print("="*60)
