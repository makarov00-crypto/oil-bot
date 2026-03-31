import Foundation

struct DashboardPayload: Decodable {
    let generatedAtMoscow: String?
    let portfolio: PortfolioSnapshot
    let runtime: RuntimeStatus
    let summary: SummaryData
    let news: NewsSnapshot
    let tradeReview: TradeReview

    enum CodingKeys: String, CodingKey {
        case generatedAtMoscow = "generated_at_moscow"
        case portfolio
        case runtime
        case summary
        case news
        case tradeReview = "trade_review"
    }
}

struct PortfolioSnapshot: Decodable {
    let mode: String?
    let generatedAtMoscow: String?
    let totalPortfolioRub: Double?
    let freeRub: Double?
    let blockedGuaranteeRub: Double?
    let botRealizedPnlRub: Double?
    let botEstimatedVariationMarginRub: Double?
    let botTotalPnlRub: Double?
    let openPositionsCount: Int?

    enum CodingKeys: String, CodingKey {
        case mode
        case generatedAtMoscow = "generated_at_moscow"
        case totalPortfolioRub = "total_portfolio_rub"
        case freeRub = "free_rub"
        case blockedGuaranteeRub = "blocked_guarantee_rub"
        case botRealizedPnlRub = "bot_realized_pnl_rub"
        case botEstimatedVariationMarginRub = "bot_estimated_variation_margin_rub"
        case botTotalPnlRub = "bot_total_pnl_rub"
        case openPositionsCount = "open_positions_count"
    }
}

struct RuntimeStatus: Decodable {
    let state: String?
    let mode: String?
    let session: String?
    let lastCycleAtMoscow: String?
    let updatedAtMoscow: String?

    enum CodingKeys: String, CodingKey {
        case state
        case mode
        case session
        case lastCycleAtMoscow = "last_cycle_at_moscow"
        case updatedAtMoscow = "updated_at_moscow"
    }
}

struct SummaryData: Decodable {
    let realizedPnlRub: Double
    let symbolsTotal: Int
    let openPositions: [OpenPosition]

    enum CodingKeys: String, CodingKey {
        case realizedPnlRub = "realized_pnl_rub"
        case symbolsTotal = "symbols_total"
        case openPositions = "open_positions"
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
    let closedReviews: [ClosedReview]

    enum CodingKeys: String, CodingKey {
        case closedCount = "closed_count"
        case wins
        case losses
        case winRate = "win_rate"
        case closedTotalPnlRub = "closed_total_pnl_rub"
        case closedReviews = "closed_reviews"
    }
}

struct ClosedReview: Decodable, Identifiable {
    let symbol: String
    let side: String
    let strategy: String
    let entryTime: String
    let exitTime: String
    let pnlRub: String
    let exitReason: String
    let verdict: String

    var id: String { "\(symbol)-\(entryTime)-\(exitTime)" }

    enum CodingKeys: String, CodingKey {
        case symbol
        case side
        case strategy
        case entryTime = "entry_time"
        case exitTime = "exit_time"
        case pnlRub = "pnl_rub"
        case exitReason = "exit_reason"
        case verdict
    }
}
