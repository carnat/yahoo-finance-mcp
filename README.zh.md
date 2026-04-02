# Yahoo Finance MCP 服务器

<div align="right">
  <a href="README.md">English</a> | <a href="README.zh.md">中文</a>
</div>

这是一个基于模型上下文协议（MCP）的服务器，提供来自 Yahoo Finance 的全面金融数据。它允许您获取股票的详细信息，包括历史价格、公司信息、财务报表、期权数据、市场新闻、分析师共识、ESG 评分、SEC 文件及市场筛选。

[![smithery badge](https://smithery.ai/badge/@Alex2Yang97/yahoo-finance-mcp)](https://smithery.ai/server/@Alex2Yang97/yahoo-finance-mcp)

## 演示

![MCP 演示](assets/demo.gif)

## MCP 工具

> **减少 Token 用量提示：** 对于价格/估值查询，优先使用 `get_fast_info` 而非 `get_stock_info`——前者仅返回约 20 个字段，而后者返回 120+ 个字段，Token 消耗减少 85–90%。需要财务比率时，优先使用 `get_financial_ratios` 而非获取完整财务报表。需要分析师摘要时，优先使用 `get_analyst_consensus` 而非 `get_recommendations`。

服务器通过模型上下文协议提供以下工具：

### 价格与市场数据

| 工具 | 描述 |
|------|-------------|
| `get_fast_info` | **轻量级。** 获取当前价格、市值、52 周区间、移动平均线和成交量（约 20 个字段）。价格查询时优先使用此工具而非 `get_stock_info`。 |
| `get_historical_stock_prices` | 获取历史 OHLCV 数据，支持自定义时间段、间隔及可选的 `columns` 过滤（例如 `["Close"]` 仅返回收盘价）。 |
| `get_stock_info` | **重量级。** 获取完整的约 120 字段公司信息字典。仅在需要深度基本面或业务描述时使用。支持可选的 `fields` 过滤器以请求特定字段。 |
| `get_price_stats` | 获取预计算的价格统计数据：今日涨跌幅、距 52 周高低点及移动平均线的距离、30 日年化波动率及 1/3/5 年 CAGR。 |
| `get_stock_actions` | 获取股票分红和拆股历史。 |
| `get_yahoo_finance_news` | 获取股票的最新新闻文章。 |

### 财务报表与比率

| 工具 | 描述 |
|------|-------------|
| `get_financial_statement` | 获取利润表、资产负债表或现金流量表（年度/季度）。支持 `ttm_income_stmt` 和 `ttm_cashflow` 获取过去十二个月数据（1 列 vs 4 列，Token 减少约 75%）。支持可选的 `line_items` 过滤器仅返回特定行项目。 |
| `get_financial_ratios` | **预计算。** 获取关键估值、盈利能力和杠杆比率：市盈率（过去/预期）、PEG、市销率、市净率、EV/EBITDA、EV/Revenue、毛利率/营业利润率/净利率、ROE、ROA、负债权益比、流动比率、速动比率、自由现金流收益率及股息收益率。 |
| `get_holder_info` | 获取主要股东、机构股东、共同基金或内幕交易信息。 |

### 分析师与预测

| 工具 | 描述 |
|------|-------------|
| `get_analyst_consensus` | 获取简洁的分析师共识：目标价（当前/低/高/均值/中位数 + 相对当前价的上涨空间）及评级分布（强烈买入/买入/持有/卖出/强烈卖出计数 + 主导评级）。 |
| `get_earnings_analysis` | 一次调用获取所有分析师前瞻性数据：EPS 预估、营收预估、EPS 趋势、盈利历史（超预期/低于预期）及增长预估。替代 5 次单独调用。 |
| `get_recommendations` | 获取原始分析师推荐或评级变更历史。 |
| `get_calendar` | 获取下一个财报日期（含 EPS/营收指引）及即将到来的除息日/派息日。 |

### 期权数据

| 工具 | 描述 |
|------|-------------|
| `get_option_expiration_dates` | 获取可用的期权到期日期。 |
| `get_option_chain` | 获取特定到期日期和类型（看涨/看跌）的期权链。支持 `min_strike`、`max_strike` 及 `in_the_money_only` 过滤，将 200 行期权链缩减至约 20 行价内期权。 |

### ESG 与文件

| 工具 | 描述 |
|------|-------------|
| `get_sustainability` | 获取 ESG 评分：环境、社会、治理、总 ESG、争议等级及同行组百分位。 |
| `get_sec_filings` | 获取近期 SEC 文件（10-K、10-Q、8-K），包含表单类型、申报日期和链接。 |

### 发现与筛选

| 工具 | 描述 |
|------|-------------|
| `search_ticker` | 通过公司名称、部分名称或 ISIN 搜索匹配的股票代码。解决"知道公司名但不知道代码"的问题。 |
| `screen_stocks` | 使用预定义标准筛选市场。可用筛选器：`aggressive_small_caps`（激进小盘股）、`day_gainers`（今日涨幅榜）、`day_losers`（今日跌幅榜）、`growth_technology_stocks`（成长科技股）、`most_actives`（最活跃）、`most_shorted_stocks`（最多卖空）、`small_cap_gainers`（小盘股涨幅榜）、`undervalued_growth_stocks`（低估成长股）、`undervalued_large_caps`（低估大盘股）、`conservative_foreign_funds`（保守型外国基金）、`high_yield_bond`（高收益债券）、`portfolio_anchors`（投资组合核心）、`solid_large_growth_funds`（稳健大盘成长基金）、`solid_midcap_growth_funds`（稳健中盘成长基金）、`top_mutual_funds`（顶级共同基金）。 |

## 实际应用场景

使用此 MCP 服务器，您可以利用 Claude 进行：

### 股票分析

- **价格分析**："显示苹果公司过去 6 个月的收盘价历史。"（使用 `columns=["Close"]` 减少 Token）
- **快速价格查询**："苹果当前价格、市值和 52 周区间是多少？"（使用 `get_fast_info`）
- **财务健康**："获取微软的季度资产负债表。"
- **过去十二个月财务**："显示苹果的过去十二个月利润表。"（使用 `ttm_income_stmt` 获取单列紧凑数据）
- **关键比率**："特斯拉的市盈率、利润率和负债权益比是多少？"（使用 `get_financial_ratios`）
- **价格统计**："英伟达距 52 周高点多远，30 日波动率如何？"（使用 `get_price_stats`）
- **趋势分析**："比较亚马逊和谷歌的季度利润表。"
- **现金流分析**："显示英伟达的年度现金流量表。"

### 市场研究

- **新闻分析**："获取关于 Meta Platforms 的最新新闻文章。"
- **机构活动**："显示苹果股票的机构股东。"
- **内幕交易**："特斯拉最近的内幕交易有哪些？"
- **期权分析**："获取 SPY 在 2024-06-21 到期的价内看涨期权。"（使用 `in_the_money_only=True`）
- **分析师共识**："亚马逊的分析师一致目标价是多少？"（使用 `get_analyst_consensus`）
- **盈利展望**："苹果未来两个季度的 EPS 和营收预估是多少？"（使用 `get_earnings_analysis`）
- **日历**："微软下一个财报日和除息日是何时？"（使用 `get_calendar`）
- **ESG**："特斯拉的 ESG 评分和争议等级如何？"（使用 `get_sustainability`）
- **SEC 文件**："显示苹果最近的 10-K 和 10-Q 文件。"（使用 `get_sec_filings`）

### 发现与筛选

- **股票代码搜索**："路威酩轩的股票代码是什么？"（使用 `search_ticker`）
- **市场筛选**："今天涨幅最大的股票有哪些？"（使用 `screen_stocks`，筛选器：`day_gainers`）
- **板块筛选**："找出低估的大盘股。"（使用 `screen_stocks`，筛选器：`undervalued_large_caps`）
- **最活跃**："今天成交量最大的股票有哪些？"（使用 `screen_stocks`，筛选器：`most_actives`）

### 投资研究

- "使用微软最新的季度财务报表创建其财务健康状况的全面分析。"
- "比较可口可乐和百事可乐的分红历史和股票拆分。"
- "分析特斯拉过去一年的机构所有权变化。"
- "生成一份关于苹果股票 30 天到期的期权市场活动报告。"
- "总结过去 6 个月科技行业的最新分析师评级变更。"
- "筛选成长科技股并显示其关键财务比率。"

## 系统要求

- Python 3.11 或更高版本
- `pyproject.toml` 中列出的依赖项，包括：
  - mcp
  - yfinance
  - pandas
  - pydantic
  - 以及其他数据处理包

## 安装

1. 克隆此仓库：
   ```bash
   git clone https://github.com/Alex2Yang97/yahoo-finance-mcp.git
   cd yahoo-finance-mcp
   ```

2. 创建并激活虚拟环境，安装依赖：
   ```bash
   uv venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   uv pip install -e .
   ```

## 使用方法

### 开发模式

您可以通过运行以下命令使用 MCP Inspector 测试服务器：

```bash
uv run server.py
```

这将启动服务器并允许您测试可用工具。

### 与 Claude Desktop 集成

要将此服务器与 Claude Desktop 集成：

1. 在本地机器上安装 Claude Desktop。
2. 在本地机器上安装 VS Code。然后运行以下命令打开 `claude_desktop_config.json` 文件：
   - MacOS：`code ~/Library/Application\ Support/Claude/claude_desktop_config.json`
   - Windows：`code $env:AppData\Claude\claude_desktop_config.json`

3. 编辑 Claude Desktop 配置文件，位于：
   - macOS：
     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uv",
           "args": [
             "--directory",
             "/ABSOLUTE/PATH/TO/PARENT/FOLDER/yahoo-finance-mcp",
             "run",
             "server.py"
           ]
         }
       }
     }
     ```
   - Windows：
     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uv",
           "args": [
             "--directory",
             "C:\\ABSOLUTE\\PATH\\TO\\PARENT\\FOLDER\\yahoo-finance-mcp",
             "run",
             "server.py"
           ]
         }
       }
     }
     ```

   - **注意**：您可能需要在命令字段中填入 uv 可执行文件的完整路径。您可以通过在 MacOS/Linux 上运行 `which uv` 或在 Windows 上运行 `where uv` 来获取此路径。

4. 重启 Claude Desktop

## 许可证

MIT 