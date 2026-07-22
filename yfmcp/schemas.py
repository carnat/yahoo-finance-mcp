"""Shared tool output schemas and enum types for yahoo-finance-mcp."""

from __future__ import annotations

from enum import Enum

from yfmcp.app import TOOL_ALIASES


# Define an enum for the type of financial statement
class FinancialType(str, Enum):
    income_stmt = "income_stmt"
    quarterly_income_stmt = "quarterly_income_stmt"
    ttm_income_stmt = "ttm_income_stmt"
    balance_sheet = "balance_sheet"
    quarterly_balance_sheet = "quarterly_balance_sheet"
    cashflow = "cashflow"
    quarterly_cashflow = "quarterly_cashflow"
    ttm_cashflow = "ttm_cashflow"


class HolderType(str, Enum):
    major_holders = "major_holders"
    institutional_holders = "institutional_holders"
    mutualfund_holders = "mutualfund_holders"
    insider_transactions = "insider_transactions"
    insider_purchases = "insider_purchases"
    insider_roster_holders = "insider_roster_holders"


class RecommendationType(str, Enum):
    recommendations = "recommendations"
    upgrades_downgrades = "upgrades_downgrades"


class FilingFactType(str, Enum):
    geographic_revenue = "geographic_revenue"
    segment_revenue = "segment_revenue"
    capex = "capex"
    rd_expense = "rd_expense"
    operating_income = "operating_income"
    net_income = "net_income"
    total_revenue = "total_revenue"
    long_term_debt = "long_term_debt"
    cash = "cash"


_SIMPLE_OUTPUT_SCHEMA: dict = {"type": "object", "properties": {}, "additionalProperties": True}

_NEWS_EVENT_OUTPUT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "evidenceClass": {"type": "string"},
                    "tickerMatch": {"type": "string"},
                    "matchBasis": {"type": "string"},
                    "urlProvenance": {"type": "string"},
                    "decisionUse": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        "meta": {"type": "object"},
        "coverage": {"type": "object"},
        "sourceStatus": {"type": "object"},
    },
    "additionalProperties": True,
}

_NUMBER_OR_NULL = {"type": ["number", "null"]}
_STRING_OR_NULL = {"type": ["string", "null"]}


def _contextual_output_schema(properties: dict) -> dict:
    return {
        "type": "object",
        "properties": {
            "source": {"const": "yahoo_finance"},
            "evidenceClass": {"const": "CONTEXTUAL_MARKET_DATA"},
            "decisionGrade": {"const": False},
            "recommendedNextAction": {
                "type": "string",
                "enum": ["NONE", "RETRY_OR_REQUEST_AVAILABLE_SECTION", "CHECK_SEC_FILINGS", "CHECK_OFFICIAL_RELEASES"],
            },
            **properties,
        },
        "additionalProperties": True,
    }


_FUND_PROFILE_OUTPUT_SCHEMA = _contextual_output_schema({
    "ticker": {"type": "string"},
    "sectionsRequested": {"type": "array", "items": {"type": "string"}},
    "sectionStatus": {"type": "object", "additionalProperties": {"type": "string"}},
    "description": _STRING_OR_NULL,
    "fundOverview": {"type": ["object", "null"]},
    "topHoldings": {"type": ["array", "null"]},
    "equityHoldings": {"type": ["object", "null"]},
    "assetClasses": {"type": ["object", "null"]},
    "sectorWeights": {"type": ["array", "null"]},
    "fundOperations": {"type": ["object", "null"]},
    "bondHoldings": {"type": ["object", "null"]},
    "bondRatings": {"type": ["object", "null"]},
})

_EARNINGS_ANALYSIS_OUTPUT_SCHEMA = _contextual_output_schema({
    "ticker": {"type": "string"},
    "earningsEstimate": {"type": ["array", "null"]},
    "revenueEstimate": {"type": ["array", "null"]},
    "epsTrend": {"type": ["array", "null"]},
    "epsRevisions": {"type": ["array", "null"]},
    "earningsHistory": {"type": ["array", "null"]},
    "growthEstimates": {"type": ["array", "null"]},
})

_FINANCIAL_RATIOS_OUTPUT_SCHEMA = _contextual_output_schema({
    "ticker": {"type": "string"},
    "currency": _STRING_OR_NULL,
    "trailingPE": _NUMBER_OR_NULL,
    "forwardPE": _NUMBER_OR_NULL,
    "pegRatio": _NUMBER_OR_NULL,
    "priceToSales": _NUMBER_OR_NULL,
    "priceToBook": _NUMBER_OR_NULL,
    "enterpriseToEbitda": _NUMBER_OR_NULL,
    "enterpriseToRevenue": _NUMBER_OR_NULL,
    "grossMargins": _NUMBER_OR_NULL,
    "operatingMargins": _NUMBER_OR_NULL,
    "profitMargins": _NUMBER_OR_NULL,
    "returnOnEquity": _NUMBER_OR_NULL,
    "returnOnAssets": _NUMBER_OR_NULL,
    "debtToEquity": _NUMBER_OR_NULL,
    "currentRatio": _NUMBER_OR_NULL,
    "quickRatio": _NUMBER_OR_NULL,
    "freeCashflow": _NUMBER_OR_NULL,
    "freeCashflowYield": _NUMBER_OR_NULL,
    "dividendYield": _NUMBER_OR_NULL,
    "payoutRatio": _NUMBER_OR_NULL,
    "earningsGrowth": _NUMBER_OR_NULL,
    "revenueGrowth": _NUMBER_OR_NULL,
    "valuationHistory": {"type": ["array", "null"], "items": {"type": "object"}},
    "valuationFrequency": _STRING_OR_NULL,
    "historyPeriodsRequested": {"type": ["integer", "null"]},
    "valuationHistoryStatus": _STRING_OR_NULL,
})

_SHARE_COUNT_TREND_OUTPUT_SCHEMA = _contextual_output_schema({
    "ticker": {"type": "string"},
    "status": {"type": "string"},
    "startDate": _STRING_OR_NULL,
    "endDate": _STRING_OR_NULL,
    "dataDate": _STRING_OR_NULL,
    "firstShares": _NUMBER_OR_NULL,
    "currentShares": _NUMBER_OR_NULL,
    "changeShares": _NUMBER_OR_NULL,
    "changePct": _NUMBER_OR_NULL,
    "sampleCount": {"type": ["integer", "null"]},
    "returnedSampleCount": {"type": ["integer", "null"]},
    "truncated": {"type": ["boolean", "null"]},
    "observations": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {"date": {"type": "string"}, "shares": {"type": "number"}},
            "additionalProperties": True,
        },
    },
})

_COMPANY_CALENDAR_OUTPUT_SCHEMA = _contextual_output_schema({
    "ticker": {"type": "string"},
    "mode": {"type": "string", "enum": ["upcoming", "history"]},
    "status": _STRING_OR_NULL,
    "items": {"type": ["array", "null"]},
    "calendar": {"type": ["object", "null"]},
    "limit": {"type": ["integer", "null"]},
    "offset": {"type": ["integer", "null"]},
    "hasMore": {"type": ["boolean", "null"]},
    "confirmationStatus": {"const": "UNVERIFIED"},
    "earningsDateConfirmed": {"type": ["boolean", "null"]},
    "earningsDateSource": _STRING_OR_NULL,
    "providerMethod": _STRING_OR_NULL,
})

_MARKET_CALENDAR_OUTPUT_SCHEMA = _contextual_output_schema({
    "status": {"type": "string"},
    "eventType": {"type": "string", "enum": ["earnings", "economic", "ipo", "splits"]},
    "startDate": {"type": "string"},
    "endDate": {"type": "string"},
    "limit": {"type": "integer"},
    "offset": {"type": "integer"},
    "itemCount": {"type": "integer"},
    "hasMore": {"type": "boolean"},
    "coverage": {"type": "object"},
    "items": {"type": "array"},
    "confirmationStatus": {"const": "UNVERIFIED"},
})

_TOOL_OUTPUT_SCHEMAS: dict[str, dict] = {
    "search_thai_funds": {
        "type": "object",
        "properties": {
            "status": {"type": "string"}, "scope": {"const": "PROFILE_CATALOG"},
            "candidates": {"type": "array"}, "candidateCount": {"type": "integer"},
            "resultsByFundStatus": {"type": "object"}, "nextCursors": {"type": "object"},
            "hasMore": {"type": "boolean"}, "evidenceClass": {"type": "string"},
            "decisionGrade": {"const": False},
        },
        "additionalProperties": True,
    },
    "get_thai_fund_nav": {
        "type": "object",
        "properties": {
            "status": {"type": "string"}, "identity": {"type": ["object", "null"]},
            "scope": {"type": "string"}, "dataDate": {"type": ["string", "null"]},
            "freshness": {"type": "object"}, "nav": {"type": ["object", "null"]},
            "nextCursor": {"type": ["string", "null"]}, "hasMore": {"type": "boolean"},
            "evidenceClass": {"type": "string"}, "decisionGrade": {"type": "boolean"},
        },
        "additionalProperties": True,
    },
    "get_thai_fund_nav_batch": {
        "type": "object",
        "properties": {
            "status": {"type": "string"}, "scope": {"const": "VAULT_BATCH"},
            "requestedWindow": {"type": "object"}, "items": {"type": "array"},
            "itemCount": {"type": "integer"}, "incompleteReferences": {"type": "array"},
            "dataDate": {"type": ["string", "null"]}, "evidenceClass": {"type": "string"},
            "decisionGrade": {"const": False},
        },
        "additionalProperties": True,
    },
    "get_thai_fund_factsheet": {
        "type": "object",
        "properties": {
            "status": {"type": "string"}, "identity": {"type": ["object", "null"]},
            "scope": {"type": "string"}, "sections": {"type": "object"},
            "sectionStatus": {"type": "object"}, "partialSuccess": {"type": "boolean"},
            "evidenceClass": {"type": "string"}, "decisionGrade": {"type": "boolean"},
        },
        "additionalProperties": True,
    },
    "get_thai_fund_dividend_history": {
        "type": "object",
        "properties": {
            "status": {"type": "string"}, "identity": {"type": ["object", "null"]},
            "scope": {"const": "PROJECT"}, "dataDate": {"type": ["string", "null"]},
            "dividends": {"type": "array"}, "nextCursor": {"type": ["string", "null"]},
            "hasMore": {"type": "boolean"}, "evidenceClass": {"type": "string"},
            "decisionGrade": {"const": False},
        },
        "additionalProperties": True,
    },
    "get_historical_stock_prices": _SIMPLE_OUTPUT_SCHEMA,
    "get_stock_info": _SIMPLE_OUTPUT_SCHEMA,
    "get_yahoo_finance_news": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_stock_actions": _SIMPLE_OUTPUT_SCHEMA,
    "get_financial_statement": _SIMPLE_OUTPUT_SCHEMA,
    "get_holder_info": _SIMPLE_OUTPUT_SCHEMA,
    "get_option_expiration_dates": _SIMPLE_OUTPUT_SCHEMA,
    "get_option_chain": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'expiration': {'type': 'string'},
                        'optionType': {'type': 'string'},
                        'dataDate': {'type': 'string'},
                        'totalContracts': {'type': 'number'},
                        'returnedContracts': {'type': 'number'},
                        'truncated': {'type': 'boolean'},
                        'dataQuality': {'type': 'object'},
                        'filtersApplied': {'type': 'object'},
                        'contracts': {'type': 'array'}},
         'additionalProperties': True},
    "get_recommendations": _SIMPLE_OUTPUT_SCHEMA,
    "get_fast_info": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastPrice': {'type': 'number'},
                        'priceBasis': {'type': 'string', 'enum': ['REGULAR_MARKET_PRICE']},
                        'observationType': {'type': 'string', 'enum': ['REGULAR_MARKET_QUOTE']},
                        'priceTimestamp': {'type': ['string', 'null']},
                        'marketState': {'type': ['string', 'null']},
                        'currency': {'type': 'string'},
                        'exchange': {'type': 'string'},
                        'quoteType': {'type': 'string'},
                        'marketCap': {'type': ['number', 'null']},
                        'shares': {'type': ['number', 'null']},
                        'dayHigh': {'type': 'number'},
                        'dayLow': {'type': 'number'},
                        'yearHigh': {'type': 'number'},
                        'yearLow': {'type': 'number'},
                        'yearChange': {'type': 'number'},
                        'preMarketPrice': {'type': ['number', 'null']},
                        'postMarketPrice': {'type': ['number', 'null']},
                        'marketOpen': {'type': 'boolean'},
                        'lastTradeDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_short_interest": _SIMPLE_OUTPUT_SCHEMA,
    "get_price_stats": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastPrice': {'type': 'number'},
                        'changePct': {'type': 'number'},
                        'distFromHigh52wPct': {'type': 'number'},
                        'distFromLow52wPct': {'type': 'number'},
                        'distFrom50dmaPct': {'type': 'number'},
                        'distFrom200dmaPct': {'type': 'number'},
                        'volatility30d': {'type': 'number'},
                        'cagr1y': {'type': 'number'},
                        'cagr3y': {'type': 'number'},
                        'cagr5y': {'type': 'number'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_analyst_consensus": _SIMPLE_OUTPUT_SCHEMA,
    "get_earnings_analysis": _EARNINGS_ANALYSIS_OUTPUT_SCHEMA,
    "get_financial_ratios": _FINANCIAL_RATIOS_OUTPUT_SCHEMA,
    "analyze_share_count_trend": _SHARE_COUNT_TREND_OUTPUT_SCHEMA,
    "get_market_calendar": _MARKET_CALENDAR_OUTPUT_SCHEMA,
    "get_calendar": _COMPANY_CALENDAR_OUTPUT_SCHEMA,
    "search_ticker": _SIMPLE_OUTPUT_SCHEMA,
    "screen_stocks": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_data": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'factType': {'type': 'string'},
                        'region': {'type': ['string', 'null']},
                        'period': {'type': ['string', 'null']},
                        'rawValue': {'type': ['string', 'null']},
                        'rawDenominator': {'type': ['string', 'null']},
                        'unit': {'type': ['string', 'null']},
                        'unitScale': {'type': ['string', 'null']},
                        'value': {'type': ['number', 'null']},
                        'denominator': {'type': ['number', 'null']},
                        'valueRatio': {'type': ['number', 'null']},
                        'valuePct': {'type': ['number', 'null']},
                        'extractionMethod': {'type': 'string'},
                        'source': {'type': 'string'},
                        'confidence': {'type': 'string'},
                        'filingType': {'type': ['string', 'null']},
                        'filingDate': {'type': ['string', 'null']},
                        'accessionNumber': {'type': ['string', 'null']},
                        'documentUrl': {'type': ['string', 'null']},
                        'indexUrl': {'type': ['string', 'null']},
                        'primaryDocumentUrl': {'type': ['string', 'null']},
                        'evidence': {'type': ['object', 'null']},
                        'calculation': {'type': ['object', 'null']},
                        'warnings': {'type': 'array'}},
         'additionalProperties': True},
    "search_filing_text": _SIMPLE_OUTPUT_SCHEMA,
    "get_technical_indicators": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'rsi14': {'type': ['number', 'null']},
                        'macd': {'type': ['number', 'null']},
                        'macdSignal': {'type': ['number', 'null']},
                        'macdHistogram': {'type': ['number', 'null']},
                        'lastClose': {'type': ['number', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_price_slope": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'startClose': {'type': ['number', 'null']},
                        'endClose': {'type': ['number', 'null']},
                        'endRawClose': {'type': ['number', 'null']},
                        'priceBasis': {'type': 'string', 'enum': ['ADJUSTED_CLOSE', 'UNADJUSTED_CLOSE']},
                        'observationType': {'type': 'string', 'enum': ['DAILY_PRICE_BAR']},
                        'slopePct': {'type': ['number', 'null']},
                        'direction': {'type': 'string'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_volume_ratio": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'ratio10d': {'type': ['number', 'null']},
                        'ratio90d': {'type': ['number', 'null']},
                        'volumeFlag': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_ma_position": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'lastClose': {'type': ['number', 'null']},
                        'sma50': {'type': ['number', 'null']},
                        'sma200': {'type': ['number', 'null']},
                        'distFrom50dmaPct': {'type': ['number', 'null']},
                        'distFrom200dmaPct': {'type': ['number', 'null']},
                        'trend': {'type': 'string'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_credit_health": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'ebitdaUsd': {'type': ['number', 'null']},
                        'ebitdaSource': {'type': ['string', 'null']},
                        'operationalEbitdaUsd': {'type': ['number', 'null']},
                        'operationalEbitdaSource': {'type': ['string', 'null']},
                        'depreciationAmortizationUsd': {'type': ['number', 'null']},
                        'interestExpenseUsd': {'type': ['number', 'null']},
                        'interestExpenseSource': {'type': ['string', 'null']},
                        'netDebtToEbitda': {'type': ['number', 'null']},
                        'interestCoverage': {'type': ['number', 'null']},
                        'interestCoverageEbit': {'type': ['number', 'null']},
                        'interestCoverageEbitda': {'type': ['number', 'null']},
                        'debtTier': {'type': ['string', 'null']},
                        'creditStress': {'type': ['boolean', 'null']},
                        'creditStressFlag': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_short_momentum": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'sharesShort': {'type': ['number', 'null']},
                        'shortPctOfFloat': {'type': ['number', 'null']},
                        'momDelta': {'type': ['number', 'null']},
                        'direction': {'type': ['string', 'null']},
                        'squeezeRisk': {'type': ['string', 'null']},
                        'flag': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_earnings_momentum": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'revision7d': {'type': ['number', 'null']},
                        'revision30d': {'type': ['number', 'null']},
                        'revision90d': {'type': ['number', 'null']},
                        'momentumFlag': {'type': ['string', 'null']},
                        'beatRate': {'type': ['number', 'null']},
                        'avgSurprisePct': {'type': ['number', 'null']},
                        'currentBeatStreak': {'type': ['number', 'null']},
                        'forwardRevisionSignal': {'type': ['string', 'null']},
                        'compositeMomentumSignal': {'type': ['string', 'null']},
                        'compositeMethodNote': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_options_flow_summary": _SIMPLE_OUTPUT_SCHEMA,
    "get_put_hedge_candidates": _SIMPLE_OUTPUT_SCHEMA,
    "get_analyst_upgrade_radar": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'netSentiment': {'type': ['number', 'null']},
                        'mixedSignal': {'type': ['boolean', 'null']},
                        'upgrades': {'type': ['number', 'null']},
                        'upgrades30d': {'type': ['number', 'null']},
                        'downgrades': {'type': ['number', 'null']},
                        'downgrades30d': {'type': ['number', 'null']},
                        'initiations': {'type': ['number', 'null']},
                        'initiations30d': {'type': ['number', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_etf_info": _FUND_PROFILE_OUTPUT_SCHEMA,
    "get_overnight_quote": _SIMPLE_OUTPUT_SCHEMA,
    "get_options_flow_scan": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'windowLabel': {'type': 'string'},
                        'pcRatio': {'type': ['number', 'null']},
                        'ivPctile': {'type': ['number', 'null']},
                        'putVolVs10dAvg': {'type': ['number', 'null']},
                        'putVolTrend': {'type': ['string', 'null']},
                        'maxPainStrike': {'type': ['number', 'null']},
                        'bracket': {'type': ['string', 'null']},
                        'formattedBlock': {'type': ['string', 'null']},
                        'dataDate': {'type': 'string'},
                        'dataQuality': {'type': 'object'},
                        'warnings': {'type': 'array'}},
         'additionalProperties': True},
    "get_price_target_bracket": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'currentPrice': {'type': ['number', 'null']},
                        'referenceTargetPrice': {'type': ['number', 'null']},
                        'referenceTargetPct': {'type': ['number', 'null']},
                        'ioPt': {'type': ['number', 'null']},
                        'eqfPct': {'type': ['number', 'null']},
                        'bracket': {'type': ['string', 'null']},
                        'inferredTag': {'type': ['string', 'null']},
                        'tag': {'type': ['string', 'null']},
                        'tagNote': {'type': ['string', 'null']},
                        'invertedFlag': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_position_score_inputs": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        't1_inputs': {'type': 'object'},
                        't2_inputs': {'type': 'object'},
                        't4_inputs': {'type': 'object'},
                        't5_inputs': {'type': 'object'},
                        'dataDate': {'type': 'string'}},
         'additionalProperties': True},
    "get_volume_gate": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'currency': {'type': ['string', 'null']},
                        'fxRate': {'type': ['number', 'null']},
                        'lastVolume': {'type': ['number', 'null']},
                        'adv10d': {'type': ['number', 'null']},
                        'adv20d': {'type': ['number', 'null']},
                        'adv90d': {'type': ['number', 'null']},
                        'ratio20d': {'type': ['number', 'null']},
                        'gatePass': {'type': ['boolean', 'null']},
                        'dataDate': {'type': 'string'},
                        'note': {'type': ['string', 'null']}},
         'additionalProperties': True},
    "get_options_summary": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "nearestExpiry": {"type": ["string", "null"]},
            "currentPrice": {"type": ["number", "null"]},
            "atmIV": {"type": ["number", "null"]},
            "pcRatioVolume": {"type": ["number", "null"]},
            "pcRatioOI": {"type": ["number", "null"]},
            "callVolume": {"type": ["number", "null"]},
            "putVolume": {"type": ["number", "null"]},
            "callOI": {"type": ["number", "null"]},
            "putOI": {"type": ["number", "null"]},
            "maxPainStrike": {"type": ["number", "null"]},
            "dataDate": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "list_sec_filings": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_outline": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_section": _SIMPLE_OUTPUT_SCHEMA,
    "list_filing_tables": _SIMPLE_OUTPUT_SCHEMA,
    "get_filing_table": _SIMPLE_OUTPUT_SCHEMA,
    "extract_filing_fact": _SIMPLE_OUTPUT_SCHEMA,
    "index_sec_filing": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "cik": {"type": "string"},
            "filingType": {"type": "string"},
            "filingDate": {"type": ["string", "null"]},
            "acceptedAt": {"type": ["string", "null"]},
            "accessionNumber": {"type": "string"},
            "documentUrl": {"type": "string"},
            "index": {
                "type": "object",
                "properties": {
                    "sections": {"type": "array"},
                    "tables": {"type": "array"},
                    "keywordMap": {"type": "object"},
                },
                "additionalProperties": True,
            },
            "meta": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "get_sec_filing_index": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "cik": {"type": "string"},
            "filingType": {"type": "string"},
            "filingDate": {"type": ["string", "null"]},
            "acceptedAt": {"type": ["string", "null"]},
            "accessionNumber": {"type": "string"},
            "documentUrl": {"type": "string"},
            "index": {
                "type": "object",
                "properties": {
                    "sections": {"type": "array"},
                    "tables": {"type": "array"},
                    "keywordMap": {"type": "object"},
                },
                "additionalProperties": True,
            },
            "meta": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "search_company_news": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_company_press_releases": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_sec_recent_events": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_public_event_timeline": _NEWS_EVENT_OUTPUT_SCHEMA,
    "verify_company_event": _NEWS_EVENT_OUTPUT_SCHEMA,
    "get_latest_earnings_release": _SIMPLE_OUTPUT_SCHEMA,
    "index_earnings_release": _SIMPLE_OUTPUT_SCHEMA,
    "extract_earnings_metrics": _SIMPLE_OUTPUT_SCHEMA,
    "extract_guidance": _SIMPLE_OUTPUT_SCHEMA,
    "extract_management_commentary": _SIMPLE_OUTPUT_SCHEMA,
    "compare_earnings_actual_vs_estimate": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'period': {'type': ['string', 'null']},
                        'reportedPeriod': {'type': ['string', 'null']},
                        'reportedDate': {'type': ['string', 'null']},
                        'releasePublishedAt': {'type': ['string', 'null']},
                        'estimatePeriod': {'type': ['string', 'null']},
                        'periodAlignmentStatus': {'type': 'string'},
                        'actual': {'type': 'object'},
                        'estimate': {'type': 'object'},
                        'surprise': {'type': 'object'},
                        'confidence': {'type': 'string'},
                        'warnings': {'type': 'array'}},
         'additionalProperties': True},
    "list_sec_filing_exhibits": _SIMPLE_OUTPUT_SCHEMA,
    "get_sec_filing_exhibit_content": _SIMPLE_OUTPUT_SCHEMA,
    "parse_public_transcript": _SIMPLE_OUTPUT_SCHEMA,
    "get_earnings_call_transcript": _SIMPLE_OUTPUT_SCHEMA,
    "list_sec_material_filings": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "cik": {"type": "string"},
            "filings": {"type": "array"},
            "meta": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "get_sec_filing_intelligence": {
        "type": "object",
        "properties": {
            "ticker": {"type": "string"},
            "filing": {"type": "object"},
            "xbrl_available": {"type": "boolean"},
            "xbrl_facts": {"type": "object"},
            "index": {"type": "object"},
            "recommended_queries": {"type": "array"},
            "status": {"type": "object"},
        },
        "additionalProperties": True,
    },
    "get_sec_filing_section_markdown": {
        "type": "object",
        "properties": {
            "section": {"type": "string"},
            "markdown": {"type": "string"},
            "tables_in_section": {"type": "number"},
            "word_count": {"type": "number"},
            "confidence": {"type": "string"},
            "source": {"type": "string"},
            "truncated": {"type": "boolean"},
        },
        "additionalProperties": True,
    },
}

for _alias_name, _canonical_name in TOOL_ALIASES.items():
    if _canonical_name in _TOOL_OUTPUT_SCHEMAS and _alias_name not in _TOOL_OUTPUT_SCHEMAS:
        _TOOL_OUTPUT_SCHEMAS[_alias_name] = _TOOL_OUTPUT_SCHEMAS[_canonical_name]

# Canonical/alias schemas that route to existing base implementations.
_TOOL_OUTPUT_SCHEMAS.setdefault("analyze_position_signals", _TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"])
_TOOL_OUTPUT_SCHEMAS.setdefault("calculate_price_target_distance", _TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"])
_TOOL_OUTPUT_SCHEMAS.setdefault("check_volume_liquidity_threshold", _TOOL_OUTPUT_SCHEMAS["get_volume_gate"])
_TOOL_OUTPUT_SCHEMAS.setdefault("analyze_options_flow_window", _TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"])
_TOOL_OUTPUT_SCHEMAS.setdefault("summarize_options_flow", _TOOL_OUTPUT_SCHEMAS["get_options_summary"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_sec_filing_fact", _TOOL_OUTPUT_SCHEMAS["extract_filing_fact"])
_TOOL_OUTPUT_SCHEMAS.setdefault("search_sec_filing_text", _TOOL_OUTPUT_SCHEMAS["search_filing_text"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_geographic_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_segment_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_total_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_revenue_exposure", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_china_exposure", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_risk_factor_mentions", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_customer_concentration", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("extract_exposure", _SIMPLE_OUTPUT_SCHEMA)
_TOOL_OUTPUT_SCHEMAS.setdefault("query_sec_filing_index", _SIMPLE_OUTPUT_SCHEMA)


_MARKET_SNAPSHOT_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "price": {"type": "object"},
        "range": {"type": "object"},
        "trend": {"type": "object"},
        "volume": {"type": "object"},
        "risk": {"type": "object"},
        "freshness": {"type": "object"},
        "componentStatus": {"type": "object"},
        "partialSuccess": {"type": "boolean"},
        "failedComponents": {"type": "array"},
        "warnings": {"type": "array"},
    },
    "additionalProperties": True,
}


_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "serverVersion": {"type": "string"},
        "toolCount": {"type": "number"},
        "manifestVersion": {"type": "string"},
        "manifestHash": {"type": "string"},
        "schemaHash": {"type": "string"},
        "runtimeHash": {"type": "string"},
        "toolMode": {"type": "string"},
        "envelopeSchemaVersion": {"type": "string"},
        "generatedAt": {"type": "string"},
        "privacyScope": {"type": "string"},
    },
    "additionalProperties": False,
}
