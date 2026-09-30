class MeasureError(Exception):
    """
    建立 golden set 或量測時可預期的失敗 (例如 SerpApi 沒有回傳資料、batch 沒有 golden URL、shard 掃描失敗)。
    訊息本身說明原因；measure.py 印出後以錯誤碼 1 結束，不印 traceback。
    """
