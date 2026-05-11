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

                        GlassCard {
                            HStack(spacing: 10) {
                                Button {
                                    Task { await store.recoverTradeOperations(date: store.selectedDate) }
                                } label: {
                                    if store.isRecoveringTrades {
                                        ProgressView()
                                            .controlSize(.small)
                                    } else {
                                        Label("Восстановить операции", systemImage: "wrench.adjustable")
                                    }
                                }
                                .buttonStyle(.borderedProminent)
                                .tint(.orange)
                                .disabled(store.isRecoveringTrades)

                                if let message = store.tradeRecoveryMessage, !message.isEmpty {
                                    Text(message)
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                            }
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
                    HStack(spacing: 12) {
                        Button {
                            Task { await store.recoverTradeOperations(date: store.selectedDate) }
                        } label: {
                            Image(systemName: "wrench.adjustable")
                        }
                        .disabled(store.isRecoveringTrades)

                        Button {
                            Task { await store.load(date: store.selectedDate) }
                        } label: {
                            Image(systemName: "arrow.clockwise")
                        }
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
                                compactInfo("Цена", trade.price ?? "-")
                                compactInfo("Лоты", formatInt(trade.qtyLots))
                                compactInfo("Итог", isOpenEvent ? "открытие" : formatTradePnl(trade.netPnlRub ?? trade.pnlRub), tone: isOpenEvent ? .white : statusTone(forString: trade.netPnlRub ?? trade.pnlRub))
                                compactInfo("Стратегия", formatStrategyLabel(trade.strategy ?? "-"))
                                compactInfo("Что произошло", tradeEventSummary(trade))
                            }

                            if !isOpenEvent {
                                compactBlock(
                                    title: "Детали",
                                    value: "До комиссии: \(formatTradePnl(trade.grossPnlRub)) · Комиссия: \(formatTradePnl(trade.commissionRub))"
                                )
                            } else {
                                compactBlock(
                                    title: "Детали",
                                    value: "Комиссия входа: \(entryCommissionText(trade.commissionRub))"
                                )
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
                    SectionHeader(title: "Обзор сделок", subtitle: "Короткий разбор дня без лишнего шума")

                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                        MetricGlassTile(title: "Закрыто", value: "\(payload.tradeReview.closedCount)")
                        MetricGlassTile(title: "Доля прибыльных", value: String(format: "%.1f%%", payload.tradeReview.winRate))
                        MetricGlassTile(title: "Плюсовых", value: "\(payload.tradeReview.wins)", tone: .green)
                        MetricGlassTile(title: "Минусовых", value: "\(payload.tradeReview.losses)", tone: .red)
                        MetricGlassTile(title: "Итог по закрытым", value: formatRub(payload.tradeReview.closedTotalPnlRub), tone: statusTone(for: payload.tradeReview.closedTotalPnlRub))
                    }

                    reviewInfoBlock(
                        title: "Что помогло и что мешало",
                        rows: [
                            ("Лучший инструмент", bestSymbolText(payload.tradeReview.bestSymbol)),
                            ("Худший инструмент", worstSymbolText(payload.tradeReview.worstSymbol)),
                            ("Лучшая стратегия", bestStrategyText(payload.tradeReview.bestStrategy)),
                            ("Что тянет вниз", worstStrategyText(payload.tradeReview.worstStrategy)),
                            ("Режим дня", regimeText(payload.tradeReview.bestRegime)),
                        ]
                    )

                    reviewInfoBlock(
                        title: "На что смотреть сейчас",
                        rows: [
                            ("Сильное сегодня", focusText(payload.tradeReview.focusToday?.strongest.first)),
                            ("Слабое сегодня", focusText(payload.tradeReview.focusToday?.toxic.first)),
                            ("Рабочая зона", strategyRegimeText(payload.tradeReview.release1Summary?.working)),
                            ("Под наблюдением", strategyRegimeText(payload.tradeReview.release1Summary?.watch)),
                            ("Осторожно", strategyRegimeText(payload.tradeReview.release1Summary?.toxic)),
                        ]
                    )

                    allocatorDecisionsBlock(payload: payload)
                    signalObservationsBlock(payload: payload)
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
                                            Text(formatStrategyLabel(trade.strategy ?? "-"))
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        SignalPill(text: displaySignal(trade.side), raw: trade.side)
                                    }

                                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                                        compactInfo("Статус", "открыта")
                                        compactInfo("Время входа", trade.time ?? "-")
                                        compactInfo("Цена", trade.price.map { String(format: "%.4f", $0) } ?? "-")
                                        compactInfo("Комиссия входа", formatTradePnl(trade.commissionRub))
                                        compactInfo("Вход", shortText(trade.reasonDisplay ?? trade.reason ?? "-"))
                                        compactInfo("Контекст", shortText(trade.contextDisplay ?? "-"))
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
                                    Text(formatStrategyLabel(trade.strategy))
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
                                compactInfo("Вход → выход", "\(trade.entryTime) → \(trade.exitTime)")
                                compactInfo("Режим", displaySession(trade.session))
                                compactInfo("Лоты", formatInt(trade.qtyLots))
                                compactInfo("Итог", formatTradePnl(trade.netPnlRub ?? trade.pnlRub), tone: statusTone(forString: trade.netPnlRub ?? trade.pnlRub))
                            }

                            Divider().overlay(Color.white.opacity(0.08))

                            compactBlock(title: "Вход", value: shortText(trade.entryContextDisplay ?? trade.entryReason ?? "-"))
                            compactBlock(title: "Выход", value: shortText(trade.exitReason))
                            compactBlock(title: "Детали", value: "До комиссии: \(formatTradePnl(trade.grossPnlRub)) · Комиссия: \(formatTradePnl(trade.commissionRub))")
                            compactBlock(title: "Вердикт", value: trade.verdict)
                        }
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func reviewInfoBlock(title: String, rows: [(String, String)]) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title)
                .font(.headline)
            VStack(spacing: 0) {
                ForEach(Array(rows.enumerated()), id: \.offset) { index, row in
                    HStack(alignment: .top, spacing: 12) {
                        Text(row.0)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .frame(width: 118, alignment: .leading)
                        Text(row.1)
                            .font(.subheadline)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .multilineTextAlignment(.leading)
                    }
                    .padding(.vertical, 9)
                    if index < rows.count - 1 {
                        Divider().overlay(Color.white.opacity(0.08))
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func allocatorDecisionsBlock(payload: DashboardPayload) -> some View {
        let decisions = Array((payload.allocatorDecisions ?? []).prefix(6))
        if decisions.isEmpty {
            reviewInfoBlock(
                title: "Последние решения аллокатора",
                rows: [
                    ("Сегодня", "решений пока нет"),
                    ("Смысл", "тут будут только решение, причина и доступное ГО"),
                ]
            )
        } else {
            VStack(alignment: .leading, spacing: 10) {
                Text("Последние решения аллокатора")
                    .font(.headline)
                VStack(spacing: 0) {
                    ForEach(Array(decisions.enumerated()), id: \.element.id) { index, decision in
                        VStack(alignment: .leading, spacing: 6) {
                            HStack(alignment: .top, spacing: 10) {
                                Text(allocatorDecisionTitle(decision))
                                    .font(.subheadline.weight(.semibold))
                                Spacer()
                                Text(decision.timeDisplay ?? "-")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Text(allocatorDecisionDetails(decision))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                        .padding(.vertical, 9)
                        if index < decisions.count - 1 {
                            Divider().overlay(Color.white.opacity(0.08))
                        }
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func signalObservationsBlock(payload: DashboardPayload) -> some View {
        let summary = payload.signalObservations
        let items = Array((summary?.items ?? []).prefix(5))
        VStack(alignment: .leading, spacing: 12) {
            Text("Сигналы за день")
                .font(.headline)

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                MetricGlassTile(title: "Проверено", value: "\(summary?.evaluated ?? 0)/\(summary?.total ?? 0)")
                MetricGlassTile(title: "Подтвердились", value: "\(summary?.favorable ?? 0)", tone: .green)
                MetricGlassTile(title: "Упущенные шансы", value: "\(summary?.deferredFavorable ?? 0)", tone: .orange)
                MetricGlassTile(title: "Слабые выбранные", value: "\(summary?.selectedUnfavorable ?? 0)", tone: .red)
                MetricGlassTile(title: "Ждут проверки", value: "\(summary?.pending ?? 0)")
            }

            Text("Точность короткой проверки: \(String(format: "%.1f%%", summary?.favorableRate ?? 0)). Ждут проверки: \(summary?.pending ?? 0).")
                .font(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            signalObservationCombosBlock(
                title: "Лучшие связки",
                emptyText: "Нужно больше проверенных сигналов.",
                items: Array((summary?.combos?.strongest ?? []).prefix(3))
            )
            signalObservationCombosBlock(
                title: "Слабые связки",
                emptyText: "Пока нет проверенных слабых связок.",
                items: Array((summary?.combos?.weakest ?? []).prefix(3))
            )
            reviewInfoBlock(
                title: "Что делать сейчас",
                rows: Array((summary?.actions ?? ["Явных шагов по сигналам пока нет."]).prefix(3)).enumerated().map {
                    ("Шаг \($0.offset + 1)", $0.element.replacingOccurrences(of: "- ", with: "", options: .anchored))
                }
            )

            if items.isEmpty {
                Text("Новые строки появятся после выбранных и отложенных сигналов.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                VStack(spacing: 0) {
                    ForEach(Array(items.enumerated()), id: \.element.id) { index, item in
                        VStack(alignment: .leading, spacing: 6) {
                            HStack(alignment: .top, spacing: 10) {
                                Text(signalObservationTitle(item))
                                    .font(.subheadline.weight(.semibold))
                                Spacer()
                                Text(item.timeDisplay ?? "-")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Text(signalObservationDetails(item))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                        .padding(.vertical, 9)
                        if index < items.count - 1 {
                            Divider().overlay(Color.white.opacity(0.08))
                        }
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func signalObservationCombosBlock(title: String, emptyText: String, items: [SignalObservationCombo]) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.subheadline.weight(.semibold))
            if items.isEmpty {
                Text(emptyText)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                VStack(spacing: 0) {
                    ForEach(Array(items.enumerated()), id: \.element.id) { index, item in
                        VStack(alignment: .leading, spacing: 4) {
                            Text(item.label)
                                .font(.caption.weight(.semibold))
                                .fixedSize(horizontal: false, vertical: true)
                            Text(signalObservationComboDetails(item))
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                        .padding(.vertical, 7)
                        if index < items.count - 1 {
                            Divider().overlay(Color.white.opacity(0.08))
                        }
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func signalObservationLearningCombosBlock(
        title: String,
        emptyText: String,
        items: [SignalObservationLearningCombo]
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.subheadline.weight(.semibold))
            if items.isEmpty {
                Text(emptyText)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                VStack(spacing: 0) {
                    ForEach(Array(items.enumerated()), id: \.element.id) { index, item in
                        VStack(alignment: .leading, spacing: 4) {
                            Text(item.label)
                                .font(.caption.weight(.semibold))
                                .fixedSize(horizontal: false, vertical: true)
                            Text(signalObservationLearningComboDetails(item))
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                        .padding(.vertical, 7)
                        if index < items.count - 1 {
                            Divider().overlay(Color.white.opacity(0.08))
                        }
                    }
                }
            }
        }
    }

    private func filteredTrades(_ rows: [TradeEvent]) -> [TradeEvent] {
        let base = rows
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
        return "\(formatStrategyLabel(bestStrategy.strategy)) (\(String(format: "%.2f", bestStrategy.pnlRub)))"
    }

    private func worstStrategyText(_ worstStrategy: NamedStrategyPnl?) -> String {
        guard let worstStrategy else { return "-" }
        return "\(formatStrategyLabel(worstStrategy.strategy)) (\(String(format: "%.2f", worstStrategy.pnlRub)))"
    }

    private func regimeText(_ regime: NamedRegimePnl?) -> String {
        guard let regime else { return "-" }
        return "\(formatRegimeLabel(regime.regime)) (\(String(format: "%.2f", regime.pnlRub)))"
    }

    private func labelPnlText(_ item: NamedLabelPnl?) -> String {
        guard let item else { return "-" }
        return "\(strategyRegimeText(item.label)) (\(String(format: "%.2f", item.pnlRub)))"
    }

    private func focusText(_ item: StrategyFocusItem?) -> String {
        guard let item else { return "-" }
        if let count = item.count {
            return "\(strategyRegimeText(item.label)) (\(String(format: "%.2f", item.pnlRub)); \(count) сд.)"
        }
        return "\(strategyRegimeText(item.label)) (\(String(format: "%.2f", item.pnlRub)))"
    }

    private func bestSymbolText(_ bestSymbol: NamedPnl?) -> String {
        guard let bestSymbol else { return "-" }
        return "\(displayName(for: bestSymbol.symbol)) (\(String(format: "%.2f", bestSymbol.pnlRub)))"
    }

    private func worstSymbolText(_ worstSymbol: NamedPnl?) -> String {
        guard let worstSymbol else { return "-" }
        return "\(displayName(for: worstSymbol.symbol)) (\(String(format: "%.2f", worstSymbol.pnlRub)))"
    }

    private func allocatorDecisionTitle(_ decision: AllocatorDecision) -> String {
        let decisionText = decision.decisionDisplay ?? "решение"
        let symbolText = decision.symbol.map { displayName(for: $0) } ?? "-"
        let signalText = displaySignal(decision.signal)
        return "\(decisionText): \(symbolText) \(signalText)"
    }

    private func allocatorDecisionDetails(_ decision: AllocatorDecision) -> String {
        var parts: [String] = []
        if let priority = decision.priorityScore {
            parts.append("приоритет \(String(format: "%.2f", priority))")
        }
        if let edge = decision.entryEdgeScore {
            parts.append("качество входа \(String(format: "%.2f", edge))")
        }
        if let requested = decision.requestedMarginRub {
            parts.append("нужно ГО \(formatRub(requested))")
        }
        if let allocatable = decision.allocatableMarginRub {
            parts.append("доступно ГО \(formatRub(allocatable))")
        }
        if let replaced = decision.replacedSymbol, !replaced.isEmpty {
            parts.append("вытеснил \(displayName(for: replaced))")
        }
        if let reason = decision.reason, !reason.isEmpty {
            parts.append(humanizeAllocatorText(reason))
        }
        return parts.isEmpty ? "подробности появятся после следующего цикла" : parts.joined(separator: " · ")
    }

    private func signalObservationTitle(_ item: SignalObservationItem) -> String {
        let decision = item.decisionDisplay ?? "наблюдение"
        let symbol = item.displayName ?? item.symbol ?? "-"
        let signal = displaySignal(item.signal)
        return "\(decision): \(symbol) \(signal)"
    }

    private func signalObservationDetails(_ item: SignalObservationItem) -> String {
        var parts: [String] = []
        if let outcome = item.outcomeDisplay, !outcome.isEmpty {
            parts.append(outcome)
        }
        if let move = item.movePct {
            parts.append("движение \(String(format: "%.2f%%", move))")
        }
        if let priority = item.priorityScore {
            parts.append("приоритет \(String(format: "%.2f", priority))")
        }
        if let edge = item.entryEdgeScore {
            parts.append("качество входа \(String(format: "%.2f", edge))")
        }
        if let reason = item.decisionReason, !reason.isEmpty {
            parts.append(humanizeAllocatorText(reason))
        }
        return parts.isEmpty ? "подробности появятся после проверки сигнала" : parts.joined(separator: " · ")
    }

    private func signalObservationComboDetails(_ item: SignalObservationCombo) -> String {
        let sampleText = item.sampleWarning ? "\(item.evaluated) пров., мало данных" : "\(item.evaluated) пров."
        return "\(String(format: "%.1f%%", item.confirmationRate)) · \(sampleText) · среднее движение \(String(format: "%.2f%%", item.avgMovePct)) · выбрано \(item.selected) · отложено \(item.deferred)"
    }

    private func signalObservationLearningComboDetails(_ item: SignalObservationLearningCombo) -> String {
        let adjustmentText = item.avgAdjustment > 0
            ? "+\(String(format: "%.2f", item.avgAdjustment))"
            : String(format: "%.2f", item.avgAdjustment)
        return "\(item.count) корр. · бонусов \(item.bonusCount) · штрафов \(item.penaltyCount) · средняя поправка \(adjustmentText) · подтверждение \(String(format: "%.1f%%", item.confirmationRate))"
    }

    private func displayName(for symbol: String) -> String {
        symbol
    }

    private func formatStrategyLabel(_ value: String) -> String {
        switch value {
        case "reversal_15m": return "15м разворот"
        case "momentum_breakout": return "Импульсный пробой"
        case "trend_pullback": return "Откат по тренду"
        case "trend_rollover": return "Перезапуск тренда"
        case "macd_stoch_reversal": return "Переворот по MACD/RSI/Stochastic"
        case "range_break_continuation": return "Продолжение пробоя диапазона"
        case "failed_breakout": return "Ложный пробой"
        case "opening_range_breakout": return "Пробой утреннего диапазона"
        case "breakdown_continuation": return "Продолжение пробоя вниз"
        case "williams": return "Подтверждение по Williams %R"
        case "-", "": return "не определена"
        default: return value.replacingOccurrences(of: "_", with: " ")
        }
    }

    private func formatRegimeLabel(_ value: String) -> String {
        switch value {
        case "trend_expansion": return "Расширение тренда"
        case "trend_pullback": return "Откат в тренде"
        case "macd_stoch_reversal": return "Волновой переворот"
        case "impulse": return "Импульс"
        case "compression": return "Сжатие"
        case "chop": return "Пила"
        case "mixed": return "Смешанный режим"
        case "-", "": return "режим не определён"
        default: return value.replacingOccurrences(of: "_", with: " ")
        }
    }

    private func strategyRegimeText(_ value: String?) -> String {
        guard let raw = value, !raw.isEmpty, raw != "-" else { return "нет данных" }
        let parts = raw.components(separatedBy: " @ ")
        if parts.count == 2 {
            return "\(formatStrategyLabel(parts[0])) / \(formatRegimeLabel(parts[1]))"
        }
        return raw
    }

    private func entryCommissionText(_ value: String?) -> String {
        guard let value, !value.isEmpty else { return "уточняется" }
        return formatTradePnl(value)
    }

    private func shortText(_ value: String, limit: Int = 160) -> String {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.count > limit else { return trimmed }
        return String(trimmed.prefix(limit - 1)) + "…"
    }

    private func tradeEventSummary(_ trade: TradeEvent) -> String {
        let base = trade.reasonDisplay ?? trade.reason ?? "-"
        return shortText(base)
    }

    private func loadingView(_ text: String) -> some View {
        ZStack {
            LiquidGlassBackground()
            ProgressView(text)
        }
    }
}
