# TelecomInsights — Power BI Dashboard Guide
# ============================================================

## Connection Setup

### Connect Power BI to Synapse Serverless SQL
1. Open Power BI Desktop
2. Get Data → Azure → Azure Synapse Analytics SQL
3. Server: your-synapse-workspace.sql.azuresynapse.net
4. Database: telecom_gold_synapse
5. Mode: DirectQuery (always fresh data)
6. Import these views:
   - vw_executive_kpis
   - vw_arpu_monthly_trend
   - vw_customer_360
   - vw_regional_performance
   - vw_churn_analysis
   - gold_daily_trend (direct table)

---

## Dashboard Pages — Build These 4 Pages

---

### PAGE 1: Executive Overview
Purpose: C-level summary, KPI cards at a glance

Layout (top to bottom):
```
┌─────────────────────────────────────────────────────────────┐
│  TelecomInsights — Executive Dashboard    [Month slicer]     │
├──────────┬──────────┬──────────┬──────────┬────────────────┤
│  Total   │  Avg     │  Total   │  Churn   │  Call Drop     │
│ Customers│  ARPU    │ Revenue  │  Risk %  │  Rate %        │
│  1,000   │ QAR 285  │ QAR 285K │   18%    │   3.2%         │
│  [KPI]   │  [KPI]   │  [KPI]   │  [KPI]   │   [KPI]        │
├──────────┴──────────┴──────────┴──────────┴────────────────┤
│  Revenue Trend (Line chart — by month)                       │
│  X=call_month | Y=total_revenue | Legend=segment            │
│  Source: vw_arpu_monthly_trend                              │
├──────────────────────────┬──────────────────────────────────┤
│  Customers by Segment    │  Churn Risk Distribution         │
│  (Donut chart)           │  (Bar chart)                     │
│  Source: vw_customer_360 │  Source: vw_churn_analysis       │
└──────────────────────────┴──────────────────────────────────┘
```

KPI Card configs:
- Total Customers: COUNT of customer_id from vw_customer_360
- Avg ARPU: AVG of avg_monthly_arpu
- Total Revenue: SUM of total_cdr_revenue
- Churn Risk %: churn_risk_pct from vw_executive_kpis
- Call Drop Rate: avg_drop_rate_pct from vw_executive_kpis

Line chart:
- X axis: call_month
- Y axis: total_revenue (sum)
- Legend: segment
- Format: QAR currency, 0 decimals

---

### PAGE 2: ARPU Deep Dive
Purpose: Revenue and usage analysis by segment/region/plan

Layout:
```
┌─────────────────────────────────────────────────────────────┐
│  ARPU Analysis                [Segment] [Region] [Plan] slicers │
├──────────────────────────────┬──────────────────────────────┤
│  ARPU by Segment (Bar)       │  ARPU by Region (Bar)        │
│  X=segment | Y=avg_arpu      │  X=region | Y=avg_revenue    │
│  Sorted desc                 │  Source: vw_regional_perf    │
├──────────────────────────────┴──────────────────────────────┤
│  Monthly Revenue Trend (Area chart)                         │
│  X=call_month | Y=total_revenue | Legend=plan_type          │
├──────────────────────────────┬──────────────────────────────┤
│  Data Usage by Segment (Bar) │  Call Duration by Plan (Bar) │
│  Y=avg_data_gb               │  Y=avg_call_duration_min     │
└──────────────────────────────┴──────────────────────────────┘
```

Key DAX measures to create:

```dax
-- ARPU (Average Revenue Per User)
ARPU =
AVERAGEX(
    vw_customer_360,
    vw_customer_360[avg_monthly_arpu]
)

-- Revenue Growth MoM
Revenue MoM % =
VAR CurrentRevenue = SUM(vw_arpu_monthly_trend[total_revenue])
VAR PrevRevenue =
    CALCULATE(
        SUM(vw_arpu_monthly_trend[total_revenue]),
        DATEADD(vw_arpu_monthly_trend[call_month], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentRevenue - PrevRevenue, PrevRevenue) * 100

-- High Value Revenue %
High Value Revenue % =
DIVIDE(
    CALCULATE(
        SUM(vw_customer_360[total_cdr_revenue]),
        vw_customer_360[is_high_value] = 1
    ),
    SUM(vw_customer_360[total_cdr_revenue])
) * 100
```

---

### PAGE 3: Churn Analysis
Purpose: Identify at-risk customers, prioritise retention

Layout:
```
┌─────────────────────────────────────────────────────────────┐
│  Churn Risk Analysis           [Churn Band] slicer          │
├──────────┬──────────┬──────────────────────────────────────┤
│  High    │  Medium  │  Churn Probability Distribution       │
│  Risk    │  Risk    │  (Histogram — probability_pct)        │
│   180    │   320    │  0-20% | 20-40% | 40-60% | 60-80% | 80%+ │
│  [KPI]   │  [KPI]   │                                      │
├──────────┴──────────┴──────────────────────────────────────┤
│  Churn Risk by Segment + Tenure (Matrix/Heatmap)            │
│  Rows=tenure_band | Cols=segment | Values=avg_churn_prob    │
├──────────────────────────────┬──────────────────────────────┤
│  Top 20 High Risk Customers  │  Churn vs Revenue Scatter    │
│  (Table)                     │  X=avg_monthly_arpu          │
│  customer_id | churn_prob%   │  Y=churn_probability_pct     │
│  | segment | plan | arpu     │  Size=total_calls            │
│  Source: vw_customer_360     │  Colour=churn_score_band     │
└──────────────────────────────┴──────────────────────────────┘
```

Key insights this page reveals:
- Low tenure (0-6m) + Prepaid = highest churn — target for retention offers
- High ARPU + High churn risk = VIP retention priority
- Scatter shows if high-value customers are at risk

DAX measures:
```dax
-- High Churn Risk Count
High Churn Count =
CALCULATE(
    COUNT(vw_customer_360[customer_id]),
    vw_customer_360[churn_score_band] = "High Risk"
)

-- Revenue at Risk
Revenue at Risk =
CALCULATE(
    SUM(vw_customer_360[avg_monthly_arpu]),
    vw_customer_360[churn_score_band] = "High Risk"
)

-- Avg Churn Probability
Avg Churn Prob % =
AVERAGE(vw_customer_360[churn_probability_pct])
```

---

### PAGE 4: Network & Operations
Purpose: Call quality, data usage, network performance

Layout:
```
┌─────────────────────────────────────────────────────────────┐
│  Network Performance           [Month] [Region] slicers     │
├──────────┬──────────┬──────────────────────────────────────┤
│  Total   │  Drop    │  5G vs 4G split                       │
│  Calls   │  Rate %  │  (Stacked bar by month)               │
│  500K    │  3.2%    │                                       │
├──────────┴──────────┴──────────────────────────────────────┤
│  Daily Call Volume + Revenue (Combo chart)                  │
│  X=call_date | Bar=total_calls | Line=daily_revenue         │
│  Source: gold_daily_trend                                   │
├──────────────────────────────┬──────────────────────────────┤
│  Call Volume by Hour         │  Drop Rate by Region         │
│  (Bar chart — call_hour)     │  (Bar chart)                 │
│  Shows morning/evening peaks │  Source: vw_regional_perf    │
├──────────────────────────────┴──────────────────────────────┤
│  Data Usage Trend (Area chart)                              │
│  X=call_month | Y=daily_data_gb | Legend=network_type       │
└─────────────────────────────────────────────────────────────┘
```

---

## Row Level Security (RLS) Setup

### For Regional Manager Access Control
In Power BI Desktop → Modelling → Manage Roles

```
Role: RegionalManager_Doha
Table: vw_customer_360
Filter: [region] = "Doha"

Role: RegionalManager_AlRayyan
Table: vw_customer_360
Filter: [region] = "Al Rayyan"
```

In Power BI Service:
- Assign users to roles after publishing
- Test with "View As" feature

---

## Publishing & Sharing

1. Power BI Desktop → Publish → Select your workspace
2. In Power BI Service:
   - Set data refresh: Daily at 7 AM (after ADF pipelines complete)
   - Share dashboard with stakeholders
   - Create a Power BI App for cleaner distribution
   - Enable Q&A feature — users can ask natural language questions

---

## Interview Story for This Dashboard

> "I built a 4-page Power BI dashboard connecting to Synapse Serverless SQL
> via DirectQuery — always real-time. Page 1 shows C-level KPI cards:
> ARPU, total revenue, churn risk %, call drop rate.
> Page 2 dives into ARPU by segment, region, and plan with MoM growth DAX.
> Page 3 is the most impactful — churn analysis with ML scores, identifying
> 180 high-risk customers representing QAR 52K of monthly revenue at risk.
> Page 4 covers network operations — call quality, 5G adoption, peak hours.
> RLS enforces regional manager access — each manager sees only their region.
> The whole pipeline from raw CDR to dashboard runs nightly in 90 minutes."
