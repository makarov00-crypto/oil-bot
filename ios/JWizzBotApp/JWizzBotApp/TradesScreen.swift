import SwiftUI

struct TradesScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0
    @State private var eventFilter = 0
    @State private var reviewSegment = 0

    private let eventFilters = ["Все", "Активные", "Закрытые", "История"]
    private let reviewSections = ["Сделки", "Стратегия", "Аллокатор"]

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

                    reviewInfoBlock(title: "Сейчас важно", rows: reviewNowRows(payload: payload))
                    SegmentedGlassPicker(title: "Обзор", selection: $reviewSegment, items: reviewSections)
                }
            }

            switch reviewSegment {
            case 1:
                strategyDiagnosticsContent(payload: payload)
            case 2:
                GlassCard {
                    allocatorDecisionsBlock(payload: payload)
                }
            default:
                reviewTradesContent(payload: payload)
            }
        }
    }

    @ViewBuilder
    private func reviewTradesContent(payload: DashboardPayload) -> some View {
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
    private func strategyDiagnosticsContent(payload: DashboardPayload) -> some View {
        let states = sortedSignalStates(payload)
        let active = activePositionStates(states)
        let actionable = actionableSignalStates(states)
        let blocked = allocatorBlockedStates(states)

        VStack(spacing: 16) {
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    reviewInfoBlock(
                        title: "Сводка стратегии",
                        rows: [
                            ("Инструментов", "\(states.count)"),
                            ("Держим позиции", active.isEmpty ? "нет открытых позиций" : active.map { $0.id }.prefix(4).joined(separator: " · ")),
                            ("Есть вход", actionable.isEmpty ? "нет свободных входов" : actionable.map { "\($0.id): \(displaySignal($0.lastSignal))" }.prefix(4).joined(separator: " · ")),
                            ("HOLD", "\(states.filter { normalizedSignal($0.lastSignal) == "HOLD" }.count)"),
                        ]
                    )

                    reviewInfoBlock(
                        title: "Текущие решения",
                        rows: [
                            ("Главное", actionable.isEmpty ? (active.isEmpty ? "ждать новый MACD-сигнал" : "контролировать сопровождение позиции") : "проверить свободные сигналы"),
                            ("Если нет входа", blocked.isEmpty ? "смотреть блокер стратегии" : "смотреть аллокатор"),
                            ("Выход", active.isEmpty ? "нет позиции для сопровождения" : "закрывать только по реальному развороту или стопу"),
                        ]
                    )
                }
            }

            if states.isEmpty {
                EmptyGlassState(
                    title: "Диагностики пока нет",
                    subtitle: "После следующего цикла здесь появятся решения стратегии по инструментам.",
                    systemImage: "waveform.path.ecg"
                )
            } else {
                ForEach(states) { state in
                    strategyDiagnosticCard(state)
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

    private func sortedSignalStates(_ payload: DashboardPayload) -> [InstrumentSignalState] {
        payload.states.values.sorted { $0.id.localizedCompare($1.id) == .orderedAscending }
    }

    private func activePositionStates(_ states: [InstrumentSignalState]) -> [InstrumentSignalState] {
        states.filter { normalizedSide($0.positionSide) != "FLAT" && ($0.positionQty ?? 0) > 0 }
    }

    private func actionableSignalStates(_ states: [InstrumentSignalState]) -> [InstrumentSignalState] {
        states.filter { state in
            let signal = normalizedSignal(state.lastSignal)
            let side = normalizedSide(state.positionSide)
            return signal != "HOLD" && (side == "FLAT" || side != signal)
        }
    }

    private func allocatorBlockedStates(_ states: [InstrumentSignalState]) -> [InstrumentSignalState] {
        states.filter { state in
            let summary = (state.lastAllocatorSummary ?? "").lowercased()
            return summary.contains("не хватает") || summary.contains("отложен") || summary.contains("0 лот")
        }
    }

    private func reviewNowRows(payload: DashboardPayload) -> [(String, String)] {
        let states = sortedSignalStates(payload)
        let active = activePositionStates(states)
        let actionable = actionableSignalStates(states)
        let blocked = allocatorBlockedStates(states)
        return [
            ("Активные позиции", active.isEmpty ? "открытых позиций нет" : active.map { "\($0.id): \(positionText($0))" }.prefix(3).joined(separator: " · ")),
            ("Сигналы без позиции", actionable.isEmpty ? "нет явных сигналов без позиции" : actionable.map { "\($0.id): \(displaySignal($0.lastSignal))" }.prefix(3).joined(separator: " · ")),
            ("Ограничения", blocked.isEmpty ? "аллокатор не показывает явных блокировок" : blocked.map(\.id).prefix(4).joined(separator: " · ")),
        ]
    }

    @ViewBuilder
    private func strategyDiagnosticCard(_ state: InstrumentSignalState) -> some View {
        let parts = diagnosticParts(from: state.signalSummary)
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                HStack(alignment: .top, spacing: 12) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(state.id)
                            .font(.title3.weight(.semibold))
                        Text("\(formatStrategyLabel(state.strategyName ?? state.entryStrategy ?? "-")) · \(positionText(state))")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    SignalPill(text: displaySignal(state.lastSignal), raw: state.lastSignal)
                }

                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                    compactInfo("MACD", shortText(parts.macd, limit: 90))
                    compactInfo("RSI", shortText(parts.rsi, limit: 90))
                    compactInfo("AO / поток", shortText(parts.ao, limit: 90))
                    compactInfo("Объём", shortText(parts.volume, limit: 90))
                }

                compactBlock(title: "Блокер", value: shortText(parts.blocker, limit: 150))
                compactBlock(title: "Аллокатор", value: shortText(state.lastAllocatorSummary ?? "аллокатор: нет свежего ограничения", limit: 150))
            }
        }
    }

    private func diagnosticParts(from summary: [String]) -> (macd: String, rsi: String, ao: String, volume: String, blocker: String) {
        let parts = summary
            .flatMap { $0.components(separatedBy: ";") }
            .map(cleanDiagnosticText)
            .filter { !$0.isEmpty }
        func find(_ needles: [String]) -> String? {
            parts.first { part in
                let lower = part.lowercased()
                return needles.contains { lower.contains($0) }
            }
        }
        return (
            macd: find(["macd"]) ?? "MACD: нет данных",
            rsi: find(["rsi="]) ?? "RSI: нет данных",
            ao: find(["ao="]) ?? find(["поток чайкина"]) ?? "AO/поток: нет данных",
            volume: find(["объём", "объем"]) ?? "Объём: нет данных",
            blocker: find(["главные блокеры", "late entry", "не подтверждён", "не подтвержден", "слишком"]) ?? parts.first ?? "нет явного блокера"
        )
    }

    private func cleanDiagnosticText(_ value: String) -> String {
        value
            .replacingOccurrences(of: "Сигнал ", with: "")
            .replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func normalizedSignal(_ value: String?) -> String {
        let signal = (value ?? "HOLD").uppercased()
        return ["LONG", "SHORT"].contains(signal) ? signal : "HOLD"
    }

    private func normalizedSide(_ value: String?) -> String {
        let side = (value ?? "FLAT").uppercased()
        return ["LONG", "SHORT"].contains(side) ? side : "FLAT"
    }

    private func positionText(_ state: InstrumentSignalState) -> String {
        let side = normalizedSide(state.positionSide)
        let qty = state.positionQty ?? 0
        guard side != "FLAT", qty > 0 else { return "позиции нет" }
        return "\(displaySignal(side)) · \(qty) лот."
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
