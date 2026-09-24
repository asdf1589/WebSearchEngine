from sqlalchemy import Column, Date, Integer, SmallInteger, BigInteger, Float, String, ForeignKey, Text, DateTime, Index, Boolean, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, declarative_mixin, relationship
from datetime import datetime

# 建立 Base
Base = declarative_base()

# ==========================================
# 1. 定義 Mixin: Crawler Stat (爬蟲統計)
# ==========================================
# 這個 Mixin 包含第一組 (Total, A, B) 共用的欄位
@declarative_mixin
class CrawlerStatMixin:
    # 共同的主鍵
    stat_date = Column(Date, primary_key=True)

    # 基礎數據
    discovered = Column(BigInteger, nullable=True)
    crawled    = Column(BigInteger, nullable=True)
    indexed    = Column(BigInteger, nullable=True)

    # 每日 Fetch 狀態
    fetch_ok    = Column(Integer, nullable=True)
    fetch_fail  = Column(Integer, nullable=True)
    fetch_total = Column(BigInteger, nullable=True)

    # 7天滾動統計
    fetch_ok_7    = Column(BigInteger, nullable=True)
    fetch_fail_7  = Column(BigInteger, nullable=True)
    fetch_total_7 = Column(BigInteger, nullable=True)

    # 30天滾動統計
    fetch_ok_30    = Column(BigInteger, nullable=True)
    fetch_fail_30  = Column(BigInteger, nullable=True)
    fetch_total_30 = Column(BigInteger, nullable=True)

    # Error Log
    http_error_404    = Column(Integer, nullable=True)
    http_error_404_7  = Column(Integer, nullable=True)
    http_error_404_30 = Column(Integer, nullable=True)
    http_error_500    = Column(Integer, nullable=True)
    http_error_500_7  = Column(Integer, nullable=True)
    http_error_500_30 = Column(Integer, nullable=True)

    # 當日成功率 = fetch_ok / fetch_total (僅 Total 表有值)
    request_success_rate = Column(Float, nullable=True)


# ==========================================
# 2. 定義 Mixin: Metric Coverage (覆蓋率指標)
# ==========================================
# 這個 Mixin 包含第二組 (Total, A, B) 共用的欄位
# 包含 HeadSet 與 RandomSet 的統計
# 主鍵是 (batch_id, stat_date)：同一天可以量不同 batch，同一個 batch 也可以在不同天重量。
@declarative_mixin
class MetricCoverageMixin:
    batch_id  = Column(BigInteger, primary_key=True)
    stat_date = Column(Date, primary_key=True)

    measured_at    = Column(DateTime, nullable=True)
    # stat_date 與 batch 建立日期相差幾天 (0 = 建立當天)
    batch_age_days = Column(Integer, nullable=True)
    # False: 由 --create --test 在建立 golden set 當下量的 (t0)
    # True : 之後重量的 (--batch_id / --batch_age_days / 單獨 --test)
    is_recheck     = Column(Boolean, nullable=False, default=False, server_default=text("false"))

    total           = Column(BigInteger, nullable=True)
    discovered_num  = Column(BigInteger, nullable=True)
    discovered_rate = Column(Float, nullable=True)
    crawled_num     = Column(BigInteger, nullable=True)
    crawled_rate    = Column(Float, nullable=True)
    indexed_num     = Column(BigInteger, nullable=True)
    indexed_rate    = Column(Float, nullable=True)
    ranked_num      = Column(BigInteger, nullable=True)
    ranked_rate     = Column(Float, nullable=True)

class MetricBatch(Base):
    __tablename__ = 'metric_batches'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now)
    
    meta_total_queries = Column(Integer, default=0)
    
    meta_total_urls = Column(Integer, default=0)

    meta_tag_stats = Column(JSONB, default=dict)
    
    meta_geo_counts = Column(JSONB, default=dict)

    # 關聯
    queries = relationship("MetricQuery", back_populates="batch")


class MetricQuery(Base):
    __tablename__ = 'metric_queries'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batch_id = Column(BigInteger, ForeignKey('metric_batches.id'), index=True)
    
    keyword = Column(String, nullable=False)
    geo = Column(JSONB, default=list)
    frequency = Column(Integer, default=0)

    tags = Column(JSONB, default=list)

    # 關聯：一個 Task 可能會有多個 Result (1對多)
    results = relationship("MetricURL", back_populates="query")
    batch = relationship("MetricBatch", back_populates="queries")

    __table_args__ = (
        Index('ix_metric_queries_tags', tags, postgresql_using='gin'),
    )


class MetricURL(Base):
    __tablename__ = 'metric_url'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # 連結回 Task (這樣就知道是哪個 Keyword + Geo 產生的結果)
    query_id = Column(BigInteger, ForeignKey('metric_queries.id'), index=True)

    url = Column(Text)
    rank = Column(Integer)

    is_discovered = Column(Boolean, default=False)
    is_crawled    = Column(Boolean, default=False)
    is_indexed    = Column(Boolean, default=False)
    is_ranked    = Column(Boolean, default=False)
    
    # [新增] 記錄這條 URL 屬於哪個 Shard (Team)，方便除錯
    shard_id = Column(Integer, nullable=True)

    # 經過 w3lib canonicalize_url 的寫法，與 crawler 寫進 url_state_current 的寫法相同
    url_canonical = Column(Text, nullable=True)

    # 最近一次量測時，從 crawlerdb url_state_current 帶回的欄位 (找不到則為 NULL)
    first_seen         = Column(DateTime(timezone=True), nullable=True)
    last_scheduled     = Column(DateTime(timezone=True), nullable=True)
    source             = Column(SmallInteger, nullable=True)  # 0 自然發現, 1 golden 注入, 2 pageview, 3 patrol
    robots_bits        = Column(SmallInteger, nullable=True)  # 0 未知, 1 允許, 2 被 robots.txt 禁止
    last_fail_reason   = Column(Text, nullable=True)
    num_scheduled_90d  = Column(Integer, nullable=True)
    num_fetch_fail_90d = Column(Integer, nullable=True)
    
    query = relationship("MetricQuery", back_populates="results")