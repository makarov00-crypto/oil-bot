import SwiftUI

struct TradesScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0
    @State private var eventFilter = 0

    private let eventFilters = ["Все", "Активные", "Закрытые", "История"]

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload {
                    ScreenContainer {
                        if let error = store.errorMessage {
                            GlassCard {
                                Label(error, systemImage: "wifi.exclamationmark")
                                    .font(.subheadline)
                                    .foregroundStyle(.orange)
                            }
                        }

                        DateFilterBar(
                            dates: payload.daily.availableDates,
                            selectedDate: store.selectedDate
                        ) { newDate in
                            Task { await store.selectDate(newDate) }
                        }

                        GlassCard {
                            SegmentedGlassPicker(title: "Раздел", selection: $segment, items: ["События", "Обзор"])
                        }

                        if segment == 0 {
                            eventsContent(payload: payload)
                        } else {
                            reviewsContent(payload: payload)
                        }
                    }
                    .refreshable { await store.load(date: store.selectedDate) }
                } else if store.isLoading {
                    loadingView("Загружаю сделки…")
                } else {
                    EmptyGlassState(
                        title: "Нет данных по сделкам",
                        subtitle: store.errorMessage ?? "Когда появятся сделки, они будут видны здесь.",
                        systemImage: "list.bullet.rectangle"
                    )
                    .padding()
                    .background(LiquidGlassBackground())
                }
            }
            .navigationTitle("Сделки")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        Task { await store.load(date: store.selectedDate) }
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }

    private func eventsContent(payload: DashboardPayload) -> some View {
        VStack(spacing: 16) {
            GlassCard {
                SegmentedGlassPicker(title: "Статус событий", selection: $eventFilter, items: eventFilters)
            }

            if filteredTrades(payload.trades).isEmpty {
                EmptyGlassState(
                    title: "Событий по фильтру нет",
                    subtitle: "Смени статус или выбери другую дату.",
                    systemImage: "clock.arrow.trianglehead.counterclockwise.rotate.90"
                )
            } else {
                ForEach(filteredTrades(payload.trades)) { trade in
                    let isOpenEvent = (trade.event ?? "").uppercased() == "OPEN"
                    GlassCard {
                        VStack(alignment: .leading, spacing: 12) {
                            HStack(alignment: .top) {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(trade.symbol)
                                        .font(.title3.weight(.semibold))
                                    Text(trade.time ?? "-")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                VStack(alignment: .trailing, spacing: 6) {
                                    SignalPill(text: displayEvent(trade.event), raw: trade.event)
                                    SignalPill(text: displaySignal(trade.eventStatus), raw: trade.eventStatus)
                                }
                            }

                            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                                compactInfo("Сторона", displaySignal(trade.side))
                                compactInfo("Лоты", formatInt(trade.qtyLots))
                                compactInfo("Цена", trade.price ?? "-")
                                compactInfo("Gross", isOpenEvent ? "не применяется" : formatTradePnl(trade.grossPnlRub))
                                compactInfo("Комиссия", isOpenEvent ? entryCommissionText(trade.commissionRub) : formatTradePnl(trade.commissionRub), tone: isOpenEvent ? .white : statusTone(for: -(safeDouble(trade.commissionRub) ?? 0)))
                                compactInfo("Net", isOpenEvent ? "не применяется" : formatTradePnl(trade.netPnlRub ?? trade.pnlRub), tone: isOpenEvent ? .white : statusTone(forString: trade.netPnlRub ?? trade.pnlRub))
                                compactInfo("Стратегия", trade.strategy ?? "-")
                                compactInfo("Причина", trade.reason ?? "-")
                            }
                        }
                    }
                }
            }
        }
    }

    private func reviewsContent(payload: DashboardPayload) -> some View {
        VStack(spacing: 16) {
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(title: "Обзор сделок", subtitle: "Итог по закрытым сделкам выбранного дня")

                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                        MetricGlassTile(title: "Закрыто", value: "\(payload.tradeReview.closedCount)")
                        MetricGlassTile(title: "Win rate", value: String(format: "%.1f%%", payload.tradeReview.winRate))
                        MetricGlassTile(title: "Плюсовых", value: "\(payload.tradeReview.wins)", tone: .green)
                        MetricGlassTile(title: "Минусовых", value: "\(payload.tradeReview.losses)", tone: .red)
                        MetricGlassTile(title: "Итог по закрытым", value: formatRub(payload.tradeReview.closedTotalPnlRub), tone: statusTone(for: payload.tradeReview.closedTotalPnlRub))
                        MetricGlassTile(title: "Лучшая стратегия", value: bestStrategyText(payload.tradeReview.bestStrategy))
                    }
                }
            }

            if payload.tradeReview.closedReviews.isEmpty {
                VStack(spacing: 16) {
                    EmptyGlassState(
                        title: "Закрытых сделок пока нет",
                        subtitle: payload.tradeReview.currentOpen?.isEmpty == false
                            ? "Есть открытые позиции. Они показаны ниже."
                            : "Когда появятся закрытия, они будут разобраны здесь.",
                        systemImage: "chart.bar.doc.horizontal"
                    )

                    if let currentOpen = payload.tradeReview.currentOpen, !currentOpen.isEmpty {
                        ForEach(currentOpen) { trade in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            Text(trade.symbol)
                                                .font(.title3.weight(.semibold))
                                            Text(trade.strategy ?? "-")
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        SignalPill(text: displaySignal(trade.side), raw: trade.side)
                                    }

                                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                                        compactInfo("Статус", "открыта")
                                        compactInfo("Время входа", trade.time ?? "-")
                                        compactInfo("Цена входа", trade.price.map { String(format: "%.4f", $0) } ?? "-")
                                        compactInfo("Комиссия входа", formatTradePnl(trade.commissionRub))
                                        compactInfo("Причина", trade.reason ?? "-")
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                ForEach(payload.tradeReview.closedReviews) { trade in
                    GlassCard {
                        VStack(alignment: .leading, spacing: 12) {
                            HStack(alignment: .top) {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(trade.symbol)
                                        .font(.title3.weight(.semibold))
                                    Text(trade.strategy)
                                        .font(.subheadline)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                VStack(alignment: .trailing, spacing: 6) {
                                    SignalPill(text: displaySignal(trade.side), raw: trade.side)
                                    Text(formatTradePnl(trade.netPnlRub ?? trade.pnlRub))
                                        .font(.headline.weight(.semibold))
                                        .foregroundStyle(statusTone(forString: trade.netPnlRub ?? trade.pnlRub))
                                }
                            }

                            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                                compactInfo("Вход", trade.entryTime)
                                compactInfo("Выход", trade.exitTime)
                                compactInfo("Цена входа", trade.entryPrice ?? "-")
                                compactInfo("Цена выхода", trade.exitPrice ?? "-")
                                compactInfo("Лоты", formatInt(trade.qtyLots))
                                compactInfo("Сессия", displaySession(trade.session))
                                compactInfo("Gross", formatTradePnl(trade.grossPnlRub))
                                compactInfo("Комиссия", formatTradePnl(trade.commissionRub), tone: statusTone(for: -(safeDouble(trade.commissionRub) ?? 0)))
                                compactInfo("Net", formatTradePnl(trade.netPnlRub ?? trade.pnlRub), tone: statusTone(forString: trade.netPnlRub ?? trade.pnlRub))
                            }

                            Divider().overlay(Color.white.opacity(0.08))

                            compactBlock(title: "Причина выхода", value: trade.exitReason)
                            compactBlock(title: "Вердикт", value: trade.verdict)
                        }
                    }
                }
            }
        }
    }

    private func filteredTrades(_ rows: [TradeEvent]) -> [TradeEvent] {
        let base = rows.reversed()
        switch eventFilter {
        case 1:
            return base.filter { ($0.eventStatus ?? "").lowercased() == "active" }
        case 2:
            return base.filter { ($0.eventStatus ?? "").lowercased() == "closed" }
        case 3:
            return base.filter { ($0.eventStatus ?? "").lowercased() == "history" }
        default:
            return Array(base)
        }
    }

    private func compactInfo(_ title: String, _ value: String, tone: Color = .white) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline)
                .foregroundStyle(tone)
                .lineLimit(3)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
    }

    private func compactBlock(title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline)
        }
    }

    private func bestStrategyText(_ bestStrategy: NamedStrategyPnl?) -> String {
        guard let bestStrategy else { return "-" }
        return "\(bestStrategy.strategy) (\(String(format: "%.2f", bestStrategy.pnlRub)))"
    }

    private func entryCommissionText(_ value: String?) -> String {
        guard let value, !value.isEmpty else { return "уточняется" }
        return formatTradePnl(value)
    }

    private func loadingView(_ text: String) -> some View {
        ZStack {
            LiquidGlassBackground()
            ProgressView(text)
        }
    }
}
