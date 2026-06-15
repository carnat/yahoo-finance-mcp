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
        "items": {"type": "array"},
        "meta": {"type": "object"},
    },
    "additionalProperties": True,
}

_TOOL_OUTPUT_SCHEMAS: dict[str, dict] = {
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
    "get_earnings_analysis": _SIMPLE_OUTPUT_SCHEMA,
    "get_financial_ratios": _SIMPLE_OUTPUT_SCHEMA,
    "get_calendar": {'type': 'object',
         'properties': {'ticker': {'type': 'string'},
                        'earningsDateConfirmed': {'type': ['boolean', 'null']},
                        'earningsDateSource': {'type': ['string', 'null']}},
         'additionalProperties': True},
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
    "get_etf_info": _SIMPLE_OUTPUT_SCHEMA,
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
_TOOL_OUTPUT_SCHEMAS.setdefault("get_tps_inputs", _TOOL_OUTPUT_SCHEMAS["get_position_score_inputs"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_eqf_bracket", _TOOL_OUTPUT_SCHEMAS["get_price_target_bracket"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_adv_gate", _TOOL_OUTPUT_SCHEMAS["get_volume_gate"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_dc134_options_scan", _TOOL_OUTPUT_SCHEMAS["get_options_flow_scan"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_china_revenue_pct", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_geographic_revenue", _TOOL_OUTPUT_SCHEMAS["get_filing_data"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_filing_text_search", _TOOL_OUTPUT_SCHEMAS["search_filing_text"])
_TOOL_OUTPUT_SCHEMAS.setdefault("get_filing_document", _TOOL_OUTPUT_SCHEMAS["get_filing_section"])


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
        "toolCount": {"type": "number"},
        "manifestVersion": {"type": ["string", "null"]},
        "manifestHash": {"type": ["string", "null"]},
        "buildSha": {"type": ["string", "null"]},
        "deployedAt": {"type": ["string", "null"]},
        "privacyScope": {"type": "string"},
        "canonicalToolCount": {"type": "number"},
        "deprecatedAliasCount": {"type": "number"},
        "publicSchemaGeneratedAt": {"type": ["string", "null"]},
        "workerSchemaGeneratedAt": {"type": ["string", "null"]},
        "manifestMismatch": {"type": ["boolean", "null"]},
        "staleConnectorWarning": {"type": ["string", "null"]},
    },
}
