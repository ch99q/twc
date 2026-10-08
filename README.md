# TradingView WebSocket Client

[![npm version](https://badge.fury.io/js/@ch99q%2Ftwc.svg)](https://badge.fury.io/js/@ch99q%2Ftwc)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive TypeScript/JavaScript client for TradingView's real-time WebSocket API and screener functionality. This library provides easy access to TradingView's live market data, charting capabilities, and powerful stock screening tools.

## Features

- 🚀 **Real-time WebSocket Connection** - Live market data streaming
- 📈 **Chart Data Access** - Historical and real-time price data
- 📊 **Technical Indicators** - Built-in support for popular indicators
- 🔍 **Financial Data & Quotes** - Comprehensive financial reports and metrics
- 🔍 **Advanced Screener** - Comprehensive stock screening capabilities  
- 🌐 **Cross-Platform** - Works in browsers, Node.js, Bun, and Deno
- 📝 **TypeScript Support** - Full type definitions included
- 🎯 **Simple API** - Easy-to-use interface with Promise-based operations

## Installation

```bash
# Using npm
npm install @ch99q/twc

# Using yarn
yarn add @ch99q/twc

# Using pnpm
pnpm add @ch99q/twc

# Using bun
bun add @ch99q/twc
```

For Node.js/Bun environments, you'll also need the `ws` package:

```bash
npm install ws @types/ws
```

## Quick Start

### Basic Chart Data

```typescript
import { createSession, createChart, createSeries } from "@ch99q/twc";

// Create a session
const session = await createSession();

// Create a chart
const chart = await createChart(session);

// Resolve a symbol
const symbol = await chart.resolve("AAPL", "NASDAQ");

// Create a series for daily data
const series = await createSeries(session, chart, symbol, "1D", 100);

// Access historical data
console.log("Latest price:", series.history[series.history.length - 1]);

// Stream real-time updates
for await (const update of series.stream()) {
  console.log("Price update:", update);
}

// Cleanup
await series.close();
await session.close();
```

### Stock Screener

```typescript
import { createScreener } from "@ch99q/twc";

// Create a screener for US markets
const screener = createScreener("america")
  .select("name", "close", "change", "volume", "market_cap_basic")
  .where("market_cap_basic").gt(1000000000) // Market cap > $1B
  .where("volume").gt(1000000) // Volume > 1M
  .where("change").gt(0) // Positive change
  .orderBy("volume", "desc")
  .limit(50);

// Execute the screen
const results = await screener.execute();

console.log(`Found ${results.totalCount} stocks`);
results.data.forEach(stock => {
  console.log(`${stock.name}: $${stock.close} (${stock.change}%)`);
});
```

### Technical Indicators

```typescript
import { rsiStudy, macdStudy } from "@ch99q/twc/studies";

// Add RSI indicator
const rsi = await rsiStudy(session, chart, series, 14);

// Add MACD indicator
const macd = await macdStudy(session, chart, series, 12, 26, "close", 9);

// Access indicator values
console.log("Current RSI:", rsi.history[rsi.history.length - 1]);
console.log("Current MACD:", macd.history[macd.history.length - 1]);
```

### Financial Data & Quotes

```typescript
import { createQuote } from "@ch99q/twc";

// Get comprehensive financial data for a stock
const quote = await createQuote(session, "AAPL", "NASDAQ");

console.log(`Financial data for ${quote.symbol}:`);
console.log(`Total reports: ${quote.reports.length}`);

// Filter by report type
const annualReports = quote.reports.filter(r => r.type === "annual");
const quarterlyReports = quote.reports.filter(r => r.type === "quarterly");

// Access latest annual report
const latestAnnual = annualReports[annualReports.length - 1];
console.log("Latest annual report:", {
  date: latestAnnual.date,
  revenue: latestAnnual.total_revenue,
  netIncome: latestAnnual.net_income,
  totalAssets: latestAnnual.total_assets,
  totalEquity: latestAnnual.total_equity,
  eps: latestAnnual.basic_eps
});

// Access latest quarterly report  
const latestQuarterly = quarterlyReports[quarterlyReports.length - 1];
console.log("Latest quarterly report:", {
  date: latestQuarterly.date, 
  revenue: latestQuarterly.total_revenue,
  netIncome: latestQuarterly.net_income
});
```

## API Reference

### Session Management

#### `createSession(token?, verbose?)`

Creates a new WebSocket session to TradingView.

- `token` (optional): Pro subscription token for premium data
- `verbose` (optional): Enable verbose logging
- Returns: `Promise<Session>`

```typescript
// Free data
const session = await createSession();

// Premium data (requires TradingView Pro)
const session = await createSession("your-pro-token");
```

### Chart Operations

#### `createChart(session)`

Creates a new chart instance for symbol resolution and data access.

```typescript
const chart = await createChart(session);
```

#### `chart.resolve(symbol, exchange, adjustment?)`

Resolves symbol information from the exchange.

- `adjustment` (optional): `"splits"` (default) or `"dividends"` for dividend-adjusted prices

```typescript
const symbol = await chart.resolve("AAPL", "NASDAQ");
console.log(symbol.description); // "Apple Inc"

// Dividend-adjusted data
const adjusted = await chart.resolve("AAPL", "NASDAQ", "dividends");
```

### Series Data

#### `createSeries(session, chart, symbol, timeframe, count, range?)`

Creates a data series for historical and real-time price data.

- `timeframe`: "1", "5", "15", "30", "60", "240", "1D", "1W", "1M"
- `count`: Number of bars to fetch
- `range` (optional): Time range or custom range

```typescript
// Last 100 daily bars
const series = await createSeries(session, chart, symbol, "1D", 100);

// Specific date range
const series = await createSeries(
  session, 
  chart, 
  symbol, 
  "1H", 
  0, 
  [1640995200, 1672531200] // Unix timestamps
);

// Predefined ranges
const series = await createSeries(session, chart, symbol, "1D", 0, "YTD");
```

### Studies & Indicators

Study functions create and attach technical indicators to a series. All study functions follow the pattern: `studyName(session, chart, series, ...parameters)`.

```typescript
import { rsiStudy, smaStudy, emaStudy, macdStudy, bollingerBandsStudy } from "@ch99q/twc/studies";

// Moving averages
const sma20 = await smaStudy(session, chart, series, 20);
const ema50 = await emaStudy(session, chart, series, 50);

// Oscillators
const rsi = await rsiStudy(session, chart, series, 14);
const macd = await macdStudy(session, chart, series, 12, 26, "close", 9);

// Bands
const bb = await bollingerBandsStudy(session, chart, series, 20, "close", 2.0);
```

### Financial Quotes

#### `createQuote(session, symbol, exchange)`

Retrieves comprehensive financial data and reports for a symbol.

- `symbol`: Stock symbol (e.g., "AAPL", "MSFT")
- `exchange`: Exchange name (e.g., "NASDAQ", "NYSE")
- Returns: `Promise<Quote>`

```typescript
const quote = await createQuote(session, "AAPL", "NASDAQ");

// Quote structure
interface Quote {
  id: string;           // Unique quote identifier
  symbol: string;       // Stock symbol
  exchange: string;     // Exchange name
  reports: Report[];    // Array of financial reports
}

// Report structure  
interface Report {
  type: "annual" | "quarterly";  // Report period type
  date: string;                  // Report date
  symbol: string;               // Stock symbol
  
  // Income Statement
  total_revenue: number;         // Total revenue
  net_income: number;           // Net income
  basic_eps: number;            // Basic earnings per share
  diluted_eps: number;          // Diluted earnings per share
  
  // Balance Sheet
  total_assets: number;         // Total assets
  total_equity: number;         // Total shareholders' equity
  total_debt: number;           // Total debt
  cash_n_short_term_investments: number; // Cash and equivalents
  
  // Cash Flow
  cash_f_operating_activities: number;   // Operating cash flow
  cash_f_investing_activities: number;   // Investing cash flow
  cash_f_financing_activities: number;   // Financing cash flow
  capital_expenditures: number;          // Capital expenditures
  
  // Key Ratios (calculated fields)
  asset_turnover: number;               // Asset turnover ratio
  book_value_per_share: number;         // Book value per share
  debt_to_equity_ratio_fq: number;      // Debt-to-equity ratio
  return_on_assets_fq: number;          // Return on assets
  return_on_equity_fq: number;          // Return on equity
  
  // And 200+ other financial fields...
}
```

**Available Financial Fields:**

The quote reports include comprehensive financial data with 200+ fields:

**Income Statement:** `total_revenue`, `gross_profit`, `operating_income`, `net_income`, `basic_eps`, `diluted_eps`

**Balance Sheet:** `total_assets`, `total_equity`, `total_debt`, `cash_n_short_term_investments`, `accounts_receivables_net`

**Cash Flow:** `cash_f_operating_activities`, `cash_f_investing_activities`, `capital_expenditures`, `free_cash_flow`

**Per Share Metrics:** `book_value_per_share`, `tangible_book_value_per_share`, `cash_per_share`, `revenue_per_share`

**Financial Ratios:** `debt_to_equity_ratio_fq`, `current_ratio_fq`, `return_on_assets_fq`, `return_on_equity_fq`

**Profitability:** `gross_margin`, `operating_margin`, `profit_margin`, `ebitda_margin`

See the complete field definitions in [`client.quote.ts`](./src/client.quote.ts).

### Stock Screener

#### `createScreener(...markets)`

Creates a new screener instance for the specified markets.

**Available Markets:**
`america`, `argentina`, `australia`, `austria`, `bahrain`, `bangladesh`, `belgium`, `brazil`, `canada`, `chile`, `china`, `colombia`, `croatia`, `cyprus`, `czech`, `denmark`, `egypt`, `estonia`, `finland`, `france`, `germany`, `greece`, `hongkong`, `hungary`, `iceland`, `india`, `indonesia`, `ireland`, `israel`, `italy`, `japan`, `jordan`, `kenya`, `korea`, `kuwait`, `latvia`, `lebanon`, `lithuania`, `luxembourg`, `malaysia`, `mexico`, `morocco`, `netherlands`, `newzealand`, `norway`, `pakistan`, `peru`, `philippines`, `poland`, `portugal`, `qatar`, `romania`, `russia`, `saudiarabia`, `serbia`, `singapore`, `slovakia`, `slovenia`, `southafrica`, `spain`, `srilanka`, `sweden`, `switzerland`, `taiwan`, `thailand`, `turkey`, `uae`, `uk`, `ukraine`, `vietnam`

#### Screening Methods

```typescript
const screener = createScreener("america", "europe")
  // Select columns to return
  .select("name", "close", "change", "volume")

  // Add filters
  .where("market_cap_basic").gte(1000000000)
  .where("volume").between(100000, 50000000)
  .where("sector").in(["Technology", "Healthcare"])

  // Combine filters with AND/OR
  .and(builder =>
    builder.where("price_earnings_ttm").lt(20)
           .where("debt_to_equity_fq").lt(0.5)
  )

  // Sort results
  .orderBy("market_cap_basic", "desc")

  // Limit results
  .limit(100)

  // Execute the screen
  .execute();
```

**Filter Operations:**
- `eq(value)` - Equal to
- `ne(value)` - Not equal to  
- `gt(value)` - Greater than
- `gte(value)` - Greater than or equal
- `lt(value)` - Less than
- `lte(value)` - Less than or equal
- `between(min, max)` - Between values
- `in(array)` - In array of values
- `match(pattern)` - Matches pattern
- `crosses(value)` - Crosses value
- `above(value)` - Above value
- `below(value)` - Below value

**Available Fields:**

The screener supports 200+ fields including:

**Basic Info:** `name`, `exchange`, `sector`, `industry`, `country`, `currency`

**Price Data:** `close`, `open`, `high`, `low`, `volume`, `change`, `change_abs`

**Market Data:** `market_cap_basic`, `shares_outstanding_calc`, `enterprise_value_calc`

**Financial Ratios:** `price_earnings_ttm`, `price_book_fq`, `debt_to_equity_fq`, `current_ratio_fq`

**Performance:** `Perf.1D`, `Perf.W`, `Perf.1M`, `Perf.3M`, `Perf.YTD`, `Perf.Y`

**Technical:** `RSI`, `MACD.macd`, `SMA20`, `EMA50`, `BB.upper`, `BB.lower`

**Dividends:** `dividend_yield_recent`, `dividend_payout_ratio_ttm`

See the full list in the [type definitions](./src/screener.ts).

## Examples

### Multi-Timeframe Analysis

```typescript
import { createSession, createChart, createSeries } from "@ch99q/twc";

const session = await createSession();
const chart = await createChart(session);
const symbol = await chart.resolve("BTCUSD", "BINANCE");

// Create multiple timeframes
const daily = await createSeries(session, chart, symbol, "1D", 100);
const hourly = await createSeries(session, chart, symbol, "1H", 200);
const minute = await createSeries(session, chart, symbol, "1", 500);

console.log("Daily trend:", daily.history.slice(-5));
console.log("Hourly trend:", hourly.history.slice(-10));
console.log("Recent prices:", minute.history.slice(-20));
```

### Financial Analysis with Quotes

```typescript
import { createSession, createQuote } from "@ch99q/twc";

async function analyzeCompany(symbol: string, exchange: string) {
  const session = await createSession();
  
  try {
    const quote = await createQuote(session, symbol, exchange);
    
    // Get latest annual and quarterly reports
    const annualReports = quote.reports.filter(r => r.type === "annual");
    const quarterlyReports = quote.reports.filter(r => r.type === "quarterly");
    
    const latestAnnual = annualReports[annualReports.length - 1];
    const latestQuarterly = quarterlyReports[quarterlyReports.length - 1];
    
    // Financial health metrics
    const analysis = {
      company: symbol,
      
      // Revenue & Growth
      annualRevenue: latestAnnual.total_revenue,
      quarterlyRevenue: latestQuarterly.total_revenue,
      revenueGrowth: ((latestQuarterly.total_revenue / quarterlyReports[quarterlyReports.length - 5]?.total_revenue - 1) * 100).toFixed(1),
      
      // Profitability
      netIncome: latestAnnual.net_income,
      profitMargin: ((latestAnnual.net_income / latestAnnual.total_revenue) * 100).toFixed(1),
      eps: latestAnnual.basic_eps,
      
      // Financial Position
      totalAssets: latestAnnual.total_assets,
      totalDebt: latestAnnual.total_debt,
      totalEquity: latestAnnual.total_equity,
      debtToEquity: (latestAnnual.total_debt / latestAnnual.total_equity).toFixed(2),
      
      // Returns
      roe: (latestAnnual.return_on_equity_fq * 100).toFixed(1),
      roa: (latestAnnual.return_on_assets_fq * 100).toFixed(1),
      
      // Cash Flow
      operatingCashFlow: latestAnnual.cash_f_operating_activities,
      freeCashFlow: latestAnnual.free_cash_flow,
      capex: latestAnnual.capital_expenditures,
    };
    
    console.log(`Financial Analysis for ${symbol}:`, analysis);
    
    // Investment signals
    const signals = [];
    if (parseFloat(analysis.revenueGrowth) > 15) signals.push("Strong revenue growth");
    if (parseFloat(analysis.profitMargin) > 20) signals.push("High profit margin");
    if (parseFloat(analysis.debtToEquity) < 0.3) signals.push("Low debt burden");
    if (parseFloat(analysis.roe) > 15) signals.push("Strong ROE");
    if (analysis.freeCashFlow > 0) signals.push("Positive free cash flow");
    
    console.log("Investment Signals:", signals);
    
  } finally {
    await session.close();
  }
}

// Analyze multiple companies
await Promise.all([
  analyzeCompany("AAPL", "NASDAQ"),
  analyzeCompany("MSFT", "NASDAQ"), 
  analyzeCompany("GOOGL", "NASDAQ")
]);
```

### Complex Screening

```typescript
import { createScreener } from "@ch99q/twc";

// Find growth stocks with strong fundamentals
const growthScreener = createScreener("america")
  .select(
    "name", "sector", "close", "market_cap_basic",
    "price_earnings_ttm", "revenue_growth_ttm_yoy",
    "earnings_growth_ttm_yoy", "debt_to_equity_fq"
  )
  .where("market_cap_basic").between(1e9, 100e9) // $1B - $100B
  .where("revenue_growth_ttm_yoy").gt(0.15) // 15%+ revenue growth
  .where("earnings_growth_ttm_yoy").gt(0.20) // 20%+ earnings growth
  .where("price_earnings_ttm").between(10, 30) // Reasonable P/E
  .where("debt_to_equity_fq").lt(0.4) // Low debt
  .orderBy("revenue_growth_ttm_yoy", "desc")
  .limit(25);

const results = await growthScreener.execute();

// Find dividend aristocrats
const dividendScreener = createScreener("america")
  .select(
    "name", "sector", "dividend_yield_recent",
    "dividend_payout_ratio_ttm", "dividend_growth_5y"
  )
  .where("dividend_yield_recent").between(0.02, 0.08) // 2-8% yield
  .where("dividend_payout_ratio_ttm").lt(0.7) // Sustainable payout
  .where("dividend_growth_5y").gt(0.05) // Growing dividends
  .orderBy("dividend_yield_recent", "desc");

const dividendStocks = await dividendScreener.execute();
```

### Real-time Monitoring

```typescript
import { createSession, createChart, createSeries } from "@ch99q/twc";
import { rsiStudy, macdStudy } from "@ch99q/twc/studies";

async function monitorStock(symbol: string, exchange: string) {
  const session = await createSession();
  const chart = await createChart(session);
  const resolvedSymbol = await chart.resolve(symbol, exchange);

  // Create 1-minute series
  const series = await createSeries(session, chart, resolvedSymbol, "1", 100);

  // Add indicators
  const rsi = await rsiStudy(session, chart, series, 14);
  const macd = await macdStudy(session, chart, series, 12, 26, "close", 9);

  console.log(`Monitoring ${symbol}...`);

  // Stream updates
  for await (const update of series.stream()) {
    const [timestamp, open, high, low, close, volume] = update;
    const currentRSI = rsi.history[rsi.history.length - 1];
    const currentMACD = macd.history[macd.history.length - 1];

    console.log({
      time: new Date(timestamp * 1000),
      price: close,
      volume,
      rsi: currentRSI?.[1],
      macd: currentMACD?.[1]
    });

    // Alert conditions
    if (currentRSI?.[1] > 70) {
      console.log("🔴 OVERBOUGHT: RSI > 70");
    } else if (currentRSI?.[1] < 30) {
      console.log("🟢 OVERSOLD: RSI < 30");
    }
  }
}

// Monitor Apple stock
await monitorStock("AAPL", "NASDAQ");
```

## Error Handling

The library includes comprehensive error handling with specific error types:

```typescript
import { 
  ProtocolError, 
  CriticalError, 
  SymbolError, 
  SeriesError,
  StudyError 
} from "@ch99q/twc";

try {
  const session = await createSession();
  // ... your code
} catch (error) {
  if (error instanceof ProtocolError) {
    console.error("Protocol error:", error.message);
  } else if (error instanceof SymbolError) {
    console.error("Symbol error:", error.symbolId, error.message);
  } else if (error instanceof SeriesError) {
    console.error("Series error:", error.seriesId, error.message);
  } else {
    console.error("Unknown error:", error);
  }
}
```

## Rate Limits & Best Practices

- **Connection Limits:** Limit concurrent connections (recommended: 1-3 per application)
- **Symbol Limits:** Avoid subscribing to too many symbols simultaneously  
- **Reconnection:** Implement exponential backoff for reconnections
- **Memory Management:** Always call `.close()` on series, studies, and sessions
- **Error Handling:** Implement proper error handling for production use

```typescript
// Good: Proper resource management
async function handleData() {
  const session = await createSession();
  try {
    // ... use session
  } finally {
    await session.close(); // Always cleanup
  }
}

// Good: Limit concurrent subscriptions  
const maxSymbols = 10;
const symbols = ["AAPL", "GOOGL", "MSFT", /* ... */].slice(0, maxSymbols);
```

## Environment Support

- **Browser:** Chrome, Firefox, Safari, Edge (latest versions)
- **Node.js:** 18.0.0+ (requires `ws` package)  
- **Bun:** Latest version (requires `ws` package)
- **Deno:** Latest version (built-in WebSocket support)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This library is not officially affiliated with TradingView. Use at your own risk. The library is provided "as is" without warranty. Always verify data independently for trading decisions.

## Support

- 📖 [Documentation](https://github.com/ch99q/twc)
- 🐛 [Issue Tracker](https://github.com/ch99q/twc/issues)
- 💬 [Discussions](https://github.com/ch99q/twc/discussions)

---

Made with ❤️ for the trading community