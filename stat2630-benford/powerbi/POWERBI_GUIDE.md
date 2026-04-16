# Power BI 连接 MongoDB Atlas 指南

## 方法：用 MongoDB Atlas Data API（推荐，免费）

### Step 1：在 Atlas 开启 Data API

1. 登录 [cloud.mongodb.com](https://cloud.mongodb.com)
2. 左侧 → **Data API**
3. Enable Data API → 选你们的 cluster
4. 生成一个 **API Key**，复制保存好

### Step 2：在 Power BI 里用 Web Connector 拉数据

1. 打开 Power BI Desktop
2. **Get Data → Web**
3. 选 **Advanced**，填入：

**URL：**
```
https://data.mongodb-api.com/app/data-<你的appid>/endpoint/data/v1/action/find
```

**HTTP Request Headers：**
```
Content-Type: application/json
api-key: <你的API Key>
```

**Request Body (POST)：**
```json
{
  "dataSource": "<你的cluster名>",
  "database": "stat2630",
  "collection": "benford_plotdata",
  "projection": {"_id": 0}
}
```

4. 点 OK → Power Query 会拿到 JSON 数据 → 展开列 → Load

---

## 三个 Collection 说明

| Collection | 内容 | 用途 |
|-----------|------|------|
| `benford_plotdata` | 每个 label × digit 的 observed/expected/z_score | 主图表数据源 |
| `benford_summary` | 每个 label 的 MAD、Cramér's V、chi_p | KPI 卡片 |
| `benford_spark_results` | Spark 版输出（可对比） | 可选 |

---

## 建议做的 Power BI 图表

1. **Clustered Bar + Line Chart**  
   X轴：digit，柱：observed，折线：expected  
   按 label 切片（All / Fraud / Legit / Balance）

2. **Card visuals**  
   MAD 值、Cramér's V、样本量 n

3. **Heatmap / Matrix**  
   Digit × Label 的 Z-score，颜色表示偏离程度

4. **Slicer**  
   选择 label（All Transactions / Fraudulent / Legitimate）

---

## 如果 Data API 太麻烦的备选方案

直接从 R 导出 CSV → Power BI 读 CSV，同样可以。  
在 benford_main.R 末尾加一行：
```r
write.csv(combined_plotdata, "plotdata_export.csv", row.names = FALSE)
```
然后 Power BI Get Data → Text/CSV。
