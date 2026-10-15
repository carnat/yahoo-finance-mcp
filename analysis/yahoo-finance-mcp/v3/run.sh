#!/bin/sh
# Re-run the V3 subrequest measurement (needs `npm ci` in worker/ and network access to Yahoo/SEC).
set -e
cd "$(dirname "$0")"
../../../worker/node_modules/.bin/esbuild entry.ts --bundle --format=esm --platform=node --outfile=yf.mjs --log-level=error
for t in getEtfInfo getStockInfo getFastInfo getPriceStats getAnalystConsensus getFinancialRatios getTechnicalIndicators \
         getPriceSlope getVolumeRatio getMaPosition getCreditHealth getShortMomentum getEarningsMomentum \
         getAnalystUpgradeRadar getPositionScoreInputs getCompanyNews getCompanyNewsAll; do
  for n in 1 5; do node measure.mjs "$t" "$n"; done
done
rm -f yf.mjs
