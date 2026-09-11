import SwiftUI

struct ShadowStrategyScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0

    private let sections = ["Сравнение", "Инструменты", "Выходы", "Позиции", "Журнал"]

    var body: some View {
        Group {
            if let workspace = store.shadowStrategyPayload {
                ScreenContainer {
                    shadowSectionPicker

                    if segment == 0 {
                        comparisonContent(workspace)
                    } else if segment == 1 {
                        instrumentsContent(workspace)
                    } else if segment == 2 {
                        exitsContent(workspace)
                    } else if segment == 3 {
                        positionsContent(workspace)
                    } else {
                        decisionsContent(workspace)
                    }
                }
                .refreshable { await store.loadShadowStrategy() }
            } else if let error = store.shadowStrategyErrorMessage {
                EmptyGlassState(
                    title: "Данные не загрузились",
                    subtitle: error,
                    systemImage: "exclamationmark.triangle"
                )
                .padding()
                .background(LiquidGlassBackground())
            } else {
                VStack(spacing: 14) {
                    ProgressView()
                    Text("Загружаю теневую стратегию…")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("Теневая стратегия")
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                Button {
                    Task { await store.loadShadowStrategy() }
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
            }
        }
        .task {
            if store.shadowStrategyPayload == nil {
                await store.loadShadowStrategy()
            }
        }
    }

    private var shadowSectionPicker: some View {
        ScrollView(.horizontal) {
            HStack(spacing: 8) {
                ForEach(sections.indices, id: \.self) { index in
                    shadowSectionButton(index)
                }
            }
        }
        .scrollIndicators(.hidden)
    }

    private func shadowSectionButton(_ index: Int) -> some View {
        let isSelected = segment == index
        let foreground = isSelected ? Color.white : Color.white.opacity(0.62)
        let fill = isSelected ? Color.cyan.opacity(0.18) : Color.white.opacity(0.05)
        let border = isSelected ? Color.cyan.opacity(0.55) : Color.white.opacity(0.08)
        return Button(sections[index]) { segment = index }
            .font(.caption.weight(.semibold))
            .foregroundStyle(foreground)
            .padding(.horizontal, 12)
            .frame(height: 36)
            .background(fill, in: RoundedRectangle(cornerRadius: 8))
            .overlay(RoundedRectangle(cornerRadius: 8).stroke(border))
    }

    @ViewBuilder
    private func comparisonContent(_ workspace: ShadowStrategyWorkspace) -> some View {
        let strategy = workspace.strategy
        let comparison = workspace.comparison

        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(
                    title: strategy.enabled ? "Наблюдение активно" : "Наблюдение выключено",
                    subtitle: "Реальные заявки не меняются. Сравнение идёт на одинаковом периоде и одном лоте."
                )
                if let settings = strategy.settings {
                    InfoRow(title: "Свечи", value: settings.timeframe ?? "1 час")
                    InfoRow(title: "Вход", value: settings.entryRule ?? "пересечение нуля AO и усиление")
                    InfoRow(
                        title: "Минимальная сила",
                        value: String(format: "%.2f обычного движения", settings.minimumStrengthATRRatio ?? 0.60)
                    )
                    InfoRow(
                        title: "Выход после ослабления",
                        value: String(format: "не более %.0f%% импульса от пика", (settings.exitAORetentionRatio ?? 0.70) * 100.0)
                    )
                    InfoRow(title: "Выход", value: settings.exitRule ?? "ослабление AO против позиции")
                }
            }
        }

        if comparison.available, let current = comparison.current, let shadow = comparison.shadow {
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(
                        title: "Одинаковый период",
                        subtitle: "\(formatShadowTime(comparison.periodStart)) — \(formatShadowTime(comparison.periodEnd))"
                    )
                    strategyMetrics(title: "Рабочая часовая стратегия", metrics: current)
                    Divider().overlay(Color.white.opacity(0.08))
                    strategyMetrics(title: "AO и поток Чайкина", metrics: shadow)
                }
            }

            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(title: "Главные различия", subtitle: "Теневая стратегия относительно рабочей.")
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                        MetricGlassTile(
                            title: "Прибыльных сделок",
                            value: formatPoints(comparison.difference?.winRatePctPoints),
                            tone: statusTone(for: comparison.difference?.winRatePctPoints)
                        )
                        MetricGlassTile(
                            title: "Разница результата",
                            value: formatRub(comparison.difference?.netResultRub1Lot),
                            tone: statusTone(for: comparison.difference?.netResultRub1Lot)
                        )
                    }
                    let exits = comparison.exitDiagnostics
                    Text("\(exits?.lossesAfterProfitableMove ?? 0) из \(exits?.lossesTotal ?? 0) убыточных теневых сделок до выхода успевали покрыть комиссию.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
            }
        } else {
            EmptyGlassState(
                title: "Сравнение ещё собирается",
                subtitle: "Нужны завершённые часовые наблюдения за одинаковый период.",
                systemImage: "chart.bar.xaxis"
            )
        }
    }

    private func strategyMetrics(title: String, metrics: ShadowStrategyMetrics) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title).font(.headline)
            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                MetricGlassTile(title: "Закрыто", value: "\(metrics.closedTrades ?? 0)")
                MetricGlassTile(
                    title: "В плюс",
                    value: "\(metrics.wins ?? 0) из \(metrics.closedTrades ?? 0) (\(formatPct(metrics.winRatePct)))"
                )
                MetricGlassTile(
                    title: "Итог на 1 лот",
                    value: formatRub(metrics.netResultRub1Lot),
                    tone: statusTone(for: metrics.netResultRub1Lot)
                )
                MetricGlassTile(title: "Удержали движение", value: formatPct(metrics.averageCapturePct))
            }
            InfoRow(title: "До комиссии", value: formatRub(metrics.grossResultRub1Lot), accent: statusTone(for: metrics.grossResultRub1Lot))
            InfoRow(title: "Комиссии", value: formatRub(metrics.commissionRub1Lot), accent: .orange)
            InfoRow(title: "Средняя прибыль", value: formatRub(metrics.averageWinRub1Lot), accent: .green)
            InfoRow(title: "Средний убыток", value: formatRub(metrics.averageLossRub1Lot), accent: .red)
        }
    }

    @ViewBuilder
    private func instrumentsContent(_ workspace: ShadowStrategyWorkspace) -> some View {
        let rows = workspace.comparison.bySymbol.sorted {
            shadowDelta($0) > shadowDelta($1)
        }
        if rows.isEmpty {
            EmptyGlassState(
                title: "Нет данных по инструментам",
                subtitle: "Разбивка появится после закрытых теневых сделок.",
                systemImage: "list.bullet.rectangle"
            )
        } else {
            ForEach(rows) { row in
                GlassCard {
                    VStack(alignment: .leading, spacing: 12) {
                        HStack(alignment: .top) {
                            Text(instrumentName(row.symbol, workspace: workspace)).font(.headline)
                            Spacer()
                            Text(formatRub(shadowDelta(row)))
                                .font(.headline.monospacedDigit())
                                .foregroundStyle(statusTone(for: shadowDelta(row)))
                        }
                        Text("Разница теневой и рабочей стратегии")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Divider().overlay(Color.white.opacity(0.08))
                        InfoRow(
                            title: "Рабочая",
                            value: "\(row.current.closedTrades ?? 0) сделок · \(row.current.wins ?? 0) в плюс (\(formatPct(row.current.winRatePct))) · \(formatRub(row.current.netResultRub1Lot))"
                        )
                        InfoRow(
                            title: "Теневая",
                            value: "\(row.shadow.closedTrades ?? 0) сделок · \(row.shadow.wins ?? 0) в плюс (\(formatPct(row.shadow.winRatePct))) · \(formatRub(row.shadow.netResultRub1Lot))"
                        )
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func exitsContent(_ workspace: ShadowStrategyWorkspace) -> some View {
        let analytics = workspace.exitAnalytics
        if analytics.available {
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(
                        title: "Что было после выхода",
                        subtitle: "Проверяем следующие закрытые часовые свечи на той же сделке и одном лоте."
                    )
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                        MetricGlassTile(title: "Проверено выходов", value: "\(analytics.closedTrades ?? 0)")
                        MetricGlassTile(title: "Удержали от максимума", value: formatPct(analytics.averageCapturePct))
                        MetricGlassTile(
                            title: "Вернули прибыль",
                            value: "\(analytics.lossesAfterProfitableMove ?? 0)",
                            tone: .orange
                        )
                        MetricGlassTile(
                            title: "Лучшее окно",
                            value: analytics.bestHorizon.map { "ещё \($0.additionalHours) ч" } ?? "не найдено"
                        )
                    }
                }
            }

            ForEach(analytics.horizons) { horizon in
                GlassCard {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack {
                            Text("Ещё \(horizon.additionalHours) час. свеч.").font(.headline)
                            Spacer()
                            Text(formatRub(horizon.deltaRub1Lot))
                                .font(.headline.monospacedDigit())
                                .foregroundStyle(statusTone(for: horizon.deltaRub1Lot))
                        }
                        InfoRow(title: "Стало лучше", value: "\(formatPct(horizon.betterPct)) · \(horizon.better ?? 0) из \(horizon.evaluated ?? 0)")
                        InfoRow(title: "Фактический итог", value: formatRub(horizon.actualNetRub1Lot))
                        InfoRow(title: "При дополнительном удержании", value: formatRub(horizon.heldNetRub1Lot))
                        Text("Положительная сумма сама по себе не означает, что надо задерживать каждый выход: важна доля сделок, где ожидание действительно помогло.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }

            ForEach(Array(analytics.trades.prefix(30))) { trade in
                GlassCard {
                    VStack(alignment: .leading, spacing: 9) {
                        HStack(alignment: .top) {
                            VStack(alignment: .leading, spacing: 3) {
                                Text(instrumentName(trade.symbol, workspace: workspace)).font(.headline)
                                Text(formatShadowTime(trade.exitTime)).font(.caption).foregroundStyle(.secondary)
                            }
                            Spacer()
                            Text(formatRub(trade.actualNetRub1Lot))
                                .font(.headline.monospacedDigit())
                                .foregroundStyle(statusTone(for: trade.actualNetRub1Lot))
                        }
                        InfoRow(title: "Удержано от максимума", value: formatPct(trade.capturePct))
                        ForEach([1, 2, 4, 8], id: \.self) { hours in
                            if let result = trade.holds[String(hours)] {
                                InfoRow(
                                    title: "Ещё \(hours) час. свеч.",
                                    value: "\(formatRub(result.netResultRub1Lot)) · разница \(formatRub(result.deltaRub1Lot))",
                                    accent: statusTone(for: result.deltaRub1Lot)
                                )
                            }
                        }
                        Text(trade.exitReason ?? "Причина выхода не сохранена")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        } else {
            EmptyGlassState(
                title: "Проверка выходов ещё собирается",
                subtitle: "Нужны завершённые теневые сделки и следующие часовые свечи.",
                systemImage: "hourglass"
            )
        }
    }

    @ViewBuilder
    private func positionsContent(_ workspace: ShadowStrategyWorkspace) -> some View {
        let positions = workspace.strategy.openPositions.sorted { ($0.candleClosedAt ?? "") > ($1.candleClosedAt ?? "") }
        if positions.isEmpty {
            EmptyGlassState(
                title: "Открытых позиций нет",
                subtitle: "Теневой вход появится после подходящей закрытой часовой свечи.",
                systemImage: "hourglass"
            )
        } else {
            ForEach(positions) { item in
                eventCard(item, workspace: workspace)
            }
        }
    }

    @ViewBuilder
    private func decisionsContent(_ workspace: ShadowStrategyWorkspace) -> some View {
        let decisions = workspace.strategy.decisions.sorted { ($0.candleClosedAt ?? "") > ($1.candleClosedAt ?? "") }
        if decisions.isEmpty {
            EmptyGlassState(
                title: "Решений пока нет",
                subtitle: "Первая запись появится после закрытия часовой свечи.",
                systemImage: "waveform.path.ecg"
            )
        } else {
            ForEach(Array(decisions.prefix(80))) { item in
                eventCard(item, workspace: workspace)
            }
        }
    }

    private func eventCard(_ item: AOChaikinShadowEvent, workspace: ShadowStrategyWorkspace) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 9) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 3) {
                        Text(instrumentName(item.symbol, workspace: workspace)).font(.headline)
                        Text(formatShadowTime(item.candleClosedAt)).font(.caption).foregroundStyle(.secondary)
                    }
                    Spacer()
                    SignalPill(text: item.decision ?? "НЕТ ВХОДА", raw: shadowDirectionTone(item.direction))
                }
                InfoRow(title: "Направление", value: displayShadowDirection(item.direction))
                InfoRow(
                    title: "Сила AO",
                    value: String(format: "%.2f обычного движения", item.aoStrengthATRRatio ?? 0.0)
                )
                InfoRow(
                    title: "Остаток импульса от пика",
                    value: item.aoPeakRetentionRatio.map { String(format: "%.1f%%", $0 * 100.0) } ?? "-"
                )
                InfoRow(title: "Поток Чайкина", value: (item.chaikinStatus ?? "нейтрален").lowercased())
                InfoRow(title: "Ослаблений AO подряд", value: "\(item.oppositeAOBars ?? 0) из 3")
                InfoRow(title: "Цена подтвердила выход", value: item.priceConfirmsExit == true ? "да" : "нет")
                if let result = item.estimatedNetRub1Lot {
                    InfoRow(title: "Результат на 1 лот", value: formatRub(result), accent: statusTone(for: result))
                }
                Text(item.reason ?? "Причина не сохранена")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if item.exitKind == "СМЕНА КОНТРАКТА" {
                    Text("Продолжение наблюдения: \(item.rolloverToSymbol ?? "новый контракт").")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.orange)
                }
            }
        }
    }

    private func shadowDelta(_ row: ShadowStrategySymbolComparison) -> Double {
        (row.shadow.netResultRub1Lot ?? 0.0) - (row.current.netResultRub1Lot ?? 0.0)
    }

    private func instrumentName(_ symbol: String, workspace: ShadowStrategyWorkspace) -> String {
        guard let name = workspace.instrumentCatalog[symbol], !name.isEmpty else { return symbol }
        return "\(symbol) — \(name)"
    }

    private func displayShadowDirection(_ raw: String?) -> String {
        switch (raw ?? "").uppercased() {
        case "LONG", "ЛОНГ": return "лонг"
        case "SHORT", "ШОРТ": return "шорт"
        default: return "нет позиции"
        }
    }

    private func shadowDirectionTone(_ raw: String?) -> String? {
        switch (raw ?? "").uppercased() {
        case "LONG", "ЛОНГ": return "LONG"
        case "SHORT", "ШОРТ": return "SHORT"
        default: return nil
        }
    }

    private func formatPoints(_ value: Double?) -> String {
        guard let value else { return "-" }
        return String(format: "%+.1f п.п.", value)
    }

    private func formatShadowTime(_ raw: String?) -> String {
        guard let raw, !raw.isEmpty else { return "-" }
        let precise = ISO8601DateFormatter()
        precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let regular = ISO8601DateFormatter()
        guard let date = precise.date(from: raw) ?? regular.date(from: raw) else { return raw }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "ru_RU")
        formatter.timeZone = TimeZone(identifier: "Europe/Moscow")
        formatter.dateFormat = "dd.MM HH:mm"
        return formatter.string(from: date)
    }
}
