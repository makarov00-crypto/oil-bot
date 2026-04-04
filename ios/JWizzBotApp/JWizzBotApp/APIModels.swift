import Foundation

private extension KeyedDecodingContainer {
    func decodeLossyStringIfPresent(forKey key: Key) throws -> String? {
        if let value = try? decodeIfPresent(String.self, forKey: key) {
            return value
        }
        if let value = try? decodeIfPresent(Double.self, forKey: key) {
            return String(format: "%.2f", value)
        }
        if let value = try? decodeIfPresent(Int.self, forKey: key) {
            return String(value)
        }
        if let value = try? decodeIfPresent(Bool.self, forKey: key) {
            return value ? "true" : "false"
        }
        return nil
    }
}

struct DashboardPayload: Decodable {
    let generatedAtMoscow: String?
    let health: HealthPayload?
    let capitalAlert: CapitalAlert?
    let portfolio: PortfolioSnapshot
    let runtime: RuntimeStatus
    let summary: SummaryData
    let news: NewsSnapshot
    let tradeReview: TradeReview
    let daily: DailyAnalytics
    let aiReview: AIReviewPayload
    let states: [String: InstrumentSignalState]
    let trades: [TradeEvent]

    enum CodingKeys: String, CodingKey {
        case generatedAtMoscow = "generated_at_moscow"
        case health
        case capitalAlert = "capital_alert"
        case portfolio
        case runtime
        case summary
        case news
        case tradeReview = "trade_review"
        case daily
        case aiReview = "ai_review"
        case states
        case trades
    }
}

struct CapitalAlert: Decodable {
    let active: Bool
    let title: String?
    let message: String?
    let symbols: [String]?
    let count: Int?
}

struct HealthPayload: Decodable {
    let ok: Bool
    let generatedAtMoscow: String?
    let symbolsCount: Int?
    let botService: ServiceStatus?
    let dashboardService: ServiceStatus?

    enum CodingKeys: String, CodingKey {
        case ok
        case generatedAtMoscow = "generated_at_moscow"
        case symbolsCount = "symbols_count"
        case botService = "bot_service"
        case dashboardService = "dashboard_service"
    }
}

struct ServiceStatus: Decodable {
    let service: String?
    let active: String?
    let enabled: String?
    let error: String?
}

struct PortfolioSnapshot: Decodable {
    let mode: String?
    let generatedAtMoscow: String?
    let reportDate: String?
    let selectedDate: String?
    let selectedDateMoscow: String?
    let totalPortfolioRub: Double?
    let freeRub: Double?
    let freeCashRub: Double?
    let blockedGuaranteeRub: Double?
    let botRealizedGrossPnlRub: Double?
    let botRealizedCommissionRub: Double?
    let botRealizedPnlRub: Double?
    let botActualVarmarginRub: Double?
    let botActualVarmarginBySymbol: [String: Double]?
    let botActualFeeRub: Double?
    let botActualCashEffectRub: Double?
    let botEstimatedVariationMarginRub: Double?
    let botTotalVarmarginRub: Double?
    let botTotalVariationMarginRub: Double?
    let botBrokerDayPnlRub: Double?
    let botTotalPnlRub: Double?
    let openPositionsCount: Int?

    enum CodingKeys: String, CodingKey {
        case mode
        case generatedAtMoscow = "generated_at_moscow"
        case reportDate = "report_date"
        case selectedDate = "selected_date"
        case selectedDateMoscow = "selected_date_moscow"
        case totalPortfolioRub = "total_portfolio_rub"
        case freeRub = "free_rub"
        case freeCashRub = "free_cash_rub"
        case blockedGuaranteeRub = "blocked_guarantee_rub"
        case botRealizedGrossPnlRub = "bot_realized_gross_pnl_rub"
        case botRealizedCommissionRub = "bot_realized_commission_rub"
        case botRealizedPnlRub = "bot_realized_pnl_rub"
        case botActualVarmarginRub = "bot_actual_varmargin_rub"
        case botActualVarmarginBySymbol = "bot_actual_varmargin_by_symbol"
        case botActualFeeRub = "bot_actual_fee_rub"
        case botActualCashEffectRub = "bot_actual_cash_effect_rub"
        case botEstimatedVariationMarginRub = "bot_estimated_variation_margin_rub"
        case botTotalVarmarginRub = "bot_total_varmargin_rub"
        case botTotalVariationMarginRub = "bot_total_variation_margin_rub"
        case botBrokerDayPnlRub = "bot_broker_day_pnl_rub"
        case botTotalPnlRub = "bot_total_pnl_rub"
        case openPositionsCount = "open_positions_count"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        mode = try container.decodeIfPresent(String.self, forKey: .mode)
        generatedAtMoscow = try container.decodeIfPresent(String.self, forKey: .generatedAtMoscow)
        reportDate = try container.decodeIfPresent(String.self, forKey: .reportDate)
        selectedDate = try container.decodeIfPresent(String.self, forKey: .selectedDate) ?? reportDate
        selectedDateMoscow = try container.decodeIfPresent(String.self, forKey: .selectedDateMoscow)
        totalPortfolioRub = try container.decodeIfPresent(Double.self, forKey: .totalPortfolioRub)
        freeRub = try container.decodeIfPresent(Double.self, forKey: .freeRub)
        freeCashRub = try container.decodeIfPresent(Double.self, forKey: .freeCashRub) ?? freeRub
        blockedGuaranteeRub = try container.decodeIfPresent(Double.self, forKey: .blockedGuaranteeRub)
        botRealizedGrossPnlRub = try container.decodeIfPresent(Double.self, forKey: .botRealizedGrossPnlRub)
        botRealizedCommissionRub = try container.decodeIfPresent(Double.self, forKey: .botRealizedCommissionRub)
        botRealizedPnlRub = try container.decodeIfPresent(Double.self, forKey: .botRealizedPnlRub)
        botActualVarmarginRub = try container.decodeIfPresent(Double.self, forKey: .botActualVarmarginRub)
        botActualVarmarginBySymbol = try container.decodeIfPresent([String: Double].self, forKey: .botActualVarmarginBySymbol)
        botActualFeeRub = try container.decodeIfPresent(Double.self, forKey: .botActualFeeRub)
        botActualCashEffectRub = try container.decodeIfPresent(Double.self, forKey: .botActualCashEffectRub)
        botEstimatedVariationMarginRub = try container.decodeIfPresent(Double.self, forKey: .botEstimatedVariationMarginRub)
        let totalVmPrimary = try container.decodeIfPresent(Double.self, forKey: .botTotalVarmarginRub)
        let totalVmAlias = try container.decodeIfPresent(Double.self, forKey: .botTotalVariationMarginRub)
        botTotalVarmarginRub = totalVmPrimary ?? totalVmAlias
        botTotalVariationMarginRub = totalVmAlias ?? totalVmPrimary
        botBrokerDayPnlRub = try container.decodeIfPresent(Double.self, forKey: .botBrokerDayPnlRub)
        botTotalPnlRub = try container.decodeIfPresent(Double.self, forKey: .botTotalPnlRub)
        openPositionsCount = try container.decodeIfPresent(Int.self, forKey: .openPositionsCount)
    }
}

struct RuntimeStatus: Decodable {
    let state: String?
    let mode: String?
    let session: String?
    let lastCycleAtMoscow: String?
    let updatedAtMoscow: String?
    let startedAtMoscow: String?
    let cycleCount: Int?
    let consecutiveErrors: Int?
    let lastError: String?

    enum CodingKeys: String, CodingKey {
        case state
        case mode
        case session
        case lastCycleAtMoscow = "last_cycle_at_moscow"
        case updatedAtMoscow = "updated_at_moscow"
        case startedAtMoscow = "started_at_moscow"
        case cycleCount = "cycle_count"
        case consecutiveErrors = "consecutive_errors"
        case lastError = "last_error"
    }
}

struct SummaryData: Decodable {
    let realizedPnlRub: Double
    let symbolsTotal: Int
    let signalCounts: SignalCounts?
    let openPositions: [OpenPosition]

    enum CodingKeys: String, CodingKey {
        case realizedPnlRub = "realized_pnl_rub"
        case symbolsTotal = "symbols_total"
        case signalCounts = "signal_counts"
        case openPositions = "open_positions"
    }
}

struct SignalCounts: Decodable {
    let long: Int
    let short: Int
    let hold: Int

    enum CodingKeys: String, CodingKey {
        case long = "LONG"
        case short = "SHORT"
        case hold = "HOLD"
    }
}

struct OpenPosition: Decodable, Identifiable {
    let symbol: String
    let side: String
    let qty: Int
    let entryPrice: Double?
    let currentPrice: Double?
    let notionalRub: Double?
    let variationMarginRub: Double?
    let pnlPct: Double?
    let strategy: String
    let lastSignal: String

    var id: String { "\(symbol)-\(side)" }

    enum CodingKeys: String, CodingKey {
        case symbol
        case side
        case qty
        case entryPrice = "entry_price"
        case currentPrice = "current_price"
        case notionalRub = "notional_rub"
        case variationMarginRub = "variation_margin_rub"
        case pnlPct = "pnl_pct"
        case strategy
        case lastSignal = "last_signal"
    }
}

struct InstrumentSignalState: Decodable, Identifiable {
    let id: String
    let lastSignal: String?
    let strategyName: String?
    let entryStrategy: String?
    let higherTFBias: String?
    let newsBias: String?
    let newsImpact: String?
    let signalSummary: [String]
    let lastError: String?
    let positionSide: String?
    let positionQty: Int?

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = decoder.codingPath.last?.stringValue ?? UUID().uuidString
        lastSignal = try container.decodeIfPresent(String.self, forKey: .lastSignal)
        strategyName = try container.decodeIfPresent(String.self, forKey: .strategyName)
        entryStrategy = try container.decodeIfPresent(String.self, forKey: .entryStrategy)
        higherTFBias = try container.decodeIfPresent(String.self, forKey: .higherTFBias)
        newsBias = try container.decodeIfPresent(String.self, forKey: .newsBias)
        newsImpact = try container.decodeIfPresent(String.self, forKey: .newsImpact)
        signalSummary = try container.decodeIfPresent([String].self, forKey: .signalSummary) ?? []
        lastError = try container.decodeIfPresent(String.self, forKey: .lastError)
        positionSide = try container.decodeIfPresent(String.self, forKey: .positionSide)
        positionQty = try container.decodeIfPresent(Int.self, forKey: .positionQty)
    }

    enum CodingKeys: String, CodingKey {
        case lastSignal = "last_signal"
        case strategyName = "last_strategy_name"
        case entryStrategy = "entry_strategy"
        case higherTFBias = "last_higher_tf_bias"
        case newsBias = "last_news_bias"
        case newsImpact = "last_news_impact"
        case signalSummary = "last_signal_summary"
        case lastError = "last_error"
        case positionSide = "position_side"
        case positionQty = "position_qty"
    }
}

struct NewsSnapshot: Decodable {
    let fetchedAtMoscow: String?
    let activeBiases: [NewsBiasItem]

    enum CodingKeys: String, CodingKey {
        case fetchedAtMoscow = "fetched_at_moscow"
        case activeBiases = "active_biases"
    }
}

struct NewsBiasItem: Decodable, Identifiable {
    let symbol: String
    let bias: String
    let strength: String
    let source: String
    let reason: String
    let messageText: String?
    let expiresAtMoscow: String?

    var id: String { "\(symbol)-\(source)-\(bias)" }

    enum CodingKeys: String, CodingKey {
        case symbol
        case bias
        case strength
        case source
        case reason
        case messageText = "message_text"
        case expiresAtMoscow = "expires_at_moscow"
    }
}

struct TradeReview: Decodable {
    let closedCount: Int
    let wins: Int
    let losses: Int
    let winRate: Double
    let closedTotalPnlRub: Double
    let bestSymbol: NamedPnl?
    let worstSymbol: NamedPnl?
    let bestStrategy: NamedStrategyPnl?
    let worstStrategy: NamedStrategyPnl?
    let closedReviews: [ClosedReview]
    let currentOpen: [OpenTradeStub]?

    enum CodingKeys: String, CodingKey {
        case closedCount = "closed_count"
        case wins
        case losses
        case winRate = "win_rate"
        case closedTotalPnlRub = "closed_total_pnl_rub"
        case bestSymbol = "best_symbol"
        case worstSymbol = "worst_symbol"
        case bestStrategy = "best_strategy"
        case worstStrategy = "worst_strategy"
        case closedReviews = "closed_reviews"
        case currentOpen = "current_open"
    }
}

struct NamedPnl: Decodable {
    let symbol: String
    let pnlRub: Double

    enum CodingKeys: String, CodingKey {
        case symbol
        case pnlRub = "pnl_rub"
    }
}

struct NamedStrategyPnl: Decodable {
    let strategy: String
    let pnlRub: Double

    enum CodingKeys: String, CodingKey {
        case strategy
        case pnlRub = "pnl_rub"
    }
}

struct OpenTradeStub: Decodable, Identifiable {
    let id: String
    let symbol: String
    let side: String?
    let strategy: String?
    let time: String?
    let price: Double?
    let commissionRub: String?
    let reason: String?
    let reasonDisplay: String?

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = UUID().uuidString
        symbol = try container.decode(String.self, forKey: .symbol)
        side = try container.decodeIfPresent(String.self, forKey: .side)
        strategy = try container.decodeIfPresent(String.self, forKey: .strategy)
        time = try container.decodeIfPresent(String.self, forKey: .time)
        price = try container.decodeIfPresent(Double.self, forKey: .price)
        commissionRub = try container.decodeLossyStringIfPresent(forKey: .commissionRub)
        reason = try container.decodeIfPresent(String.self, forKey: .reason)
        reasonDisplay = try container.decodeIfPresent(String.self, forKey: .reasonDisplay)
    }

    enum CodingKeys: String, CodingKey {
        case symbol
        case side
        case strategy
        case time
        case price
        case commissionRub = "commission_rub"
        case reason
        case reasonDisplay = "reason_display"
    }
}

struct ClosedReview: Decodable, Identifiable {
    let id: String
    let symbol: String
    let side: String
    let strategy: String
    let session: String?
    let entryTime: String
    let exitTime: String
    let entryPrice: String?
    let exitPrice: String?
    let qtyLots: Int?
    let pnlRub: String
    let grossPnlRub: String?
    let commissionRub: String?
    let netPnlRub: String?
    let entryReason: String?
    let exitReason: String
    let verdict: String

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = UUID().uuidString
        symbol = try container.decode(String.self, forKey: .symbol)
        side = try container.decode(String.self, forKey: .side)
        strategy = try container.decode(String.self, forKey: .strategy)
        session = try container.decodeIfPresent(String.self, forKey: .session)
        entryTime = try container.decode(String.self, forKey: .entryTime)
        exitTime = try container.decodeIfPresent(String.self, forKey: .exitTime)
            ?? container.decode(String.self, forKey: .closeTime)
        entryPrice = try container.decodeLossyStringIfPresent(forKey: .entryPrice)
        exitPrice = try container.decodeLossyStringIfPresent(forKey: .exitPrice)
        qtyLots = try container.decodeIfPresent(Int.self, forKey: .qtyLots)
        pnlRub = try container.decodeLossyStringIfPresent(forKey: .pnlRub) ?? "-"
        grossPnlRub = try container.decodeLossyStringIfPresent(forKey: .grossPnlRub)
        commissionRub = try container.decodeLossyStringIfPresent(forKey: .commissionRub)
        netPnlRub = try container.decodeLossyStringIfPresent(forKey: .netPnlRub)
        entryReason = try container.decodeIfPresent(String.self, forKey: .entryReason)
        exitReason = try container.decode(String.self, forKey: .exitReason)
        verdict = try container.decode(String.self, forKey: .verdict)
    }

    enum CodingKeys: String, CodingKey {
        case symbol
        case side
        case strategy
        case session
        case entryTime = "entry_time"
        case exitTime = "exit_time"
        case closeTime = "close_time"
        case entryPrice = "entry_price"
        case exitPrice = "exit_price"
        case qtyLots = "qty_lots"
        case pnlRub = "pnl_rub"
        case grossPnlRub = "gross_pnl_rub"
        case commissionRub = "commission_rub"
        case netPnlRub = "net_pnl_rub"
        case entryReason = "entry_reason"
        case exitReason = "exit_reason"
        case verdict
    }
}

struct TradeEvent: Decodable, Identifiable {
    let id: String
    let time: String?
    let symbol: String
    let event: String?
    let eventStatus: String?
    let side: String?
    let qtyLots: Int?
    let price: String?
    let pnlRub: String?
    let grossPnlRub: String?
    let commissionRub: String?
    let netPnlRub: String?
    let strategy: String?
    let reason: String?
    let reasonDisplay: String?

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = UUID().uuidString
        time = try container.decodeIfPresent(String.self, forKey: .time)
        symbol = try container.decode(String.self, forKey: .symbol)
        event = try container.decodeIfPresent(String.self, forKey: .event)
        eventStatus = try container.decodeIfPresent(String.self, forKey: .eventStatus)
        side = try container.decodeIfPresent(String.self, forKey: .side)
        qtyLots = try container.decodeIfPresent(Int.self, forKey: .qtyLots)
        price = try container.decodeLossyStringIfPresent(forKey: .price)
        pnlRub = try container.decodeLossyStringIfPresent(forKey: .pnlRub)
        grossPnlRub = try container.decodeLossyStringIfPresent(forKey: .grossPnlRub)
        commissionRub = try container.decodeLossyStringIfPresent(forKey: .commissionRub)
        netPnlRub = try container.decodeLossyStringIfPresent(forKey: .netPnlRub)
        strategy = try container.decodeIfPresent(String.self, forKey: .strategy)
        reason = try container.decodeIfPresent(String.self, forKey: .reason)
        reasonDisplay = try container.decodeIfPresent(String.self, forKey: .reasonDisplay)
    }

    enum CodingKeys: String, CodingKey {
        case time
        case symbol
        case event
        case eventStatus = "event_status"
        case side
        case qtyLots = "qty_lots"
        case price
        case pnlRub = "pnl_rub"
        case grossPnlRub = "gross_pnl_rub"
        case commissionRub = "commission_rub"
        case netPnlRub = "net_pnl_rub"
        case strategy
        case reason
        case reasonDisplay = "reason_display"
    }
}

struct DailyAnalytics: Decodable {
    let selectedDate: String
    let availableDates: [String]
    let selected: DailyPoint
    let series: [DailyPoint]

    enum CodingKeys: String, CodingKey {
        case selectedDate = "selected_date"
        case availableDates = "available_dates"
        case selected
        case series
    }
}

struct DailyPoint: Decodable, Identifiable {
    let date: String
    let closedCount: Int
    let wins: Int
    let losses: Int
    let pnlRub: Double
    let pnlPct: Double
    let cumulativePnlRub: Double
    let cumulativePnlPct: Double

    var id: String { date }

    enum CodingKeys: String, CodingKey {
        case date
        case closedCount = "closed_count"
        case wins
        case losses
        case pnlRub = "pnl_rub"
        case pnlPct = "pnl_pct"
        case cumulativePnlRub = "cumulative_pnl_rub"
        case cumulativePnlPct = "cumulative_pnl_pct"
    }
}

struct AIReviewPayload: Decodable {
    let available: Bool
    let date: String?
    let source: String?
    let content: String
    let updatedAtMoscow: String?
    let status: String?

    enum CodingKeys: String, CodingKey {
        case available
        case date
        case source
        case content
        case updatedAtMoscow = "updated_at_moscow"
        case status
    }
}
