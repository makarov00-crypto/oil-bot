import SwiftUI

struct TradeQualityScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0

    private let sections = ["Итог", "Лаборатория", "Выходы", "Гипотезы"]

    var body: some View {
        Group {
            if let payload = store.payload, let quality = payload.tradeQuality, quality.available {
                ScreenContainer {
                    GlassCard {
                        SegmentedGlassPicker(title: "Диагностика", selection: $segment, items: sections)
                    }

                    if segment == 0 {
                        overviewContent(payload: payload, quality: quality)
                    } else if segment == 1 {
                        tradesContent(payload: payload, quality: quality)
                    } else if segment == 2 {
                        exitsContent(payload: payload, quality: quality)
                    } else {
                        hypothesesContent(payload: payload, quality: quality)
                    }
                }
                .refreshable { await store.load(date: store.selectedDate) }
            } else if store.isLoading {
                VStack(spacing: 14) {
                    ProgressView()
                    Text("Загружаю качество торговли…")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(LiquidGlassBackground())
            } else {
                EmptyGlassState(
                    title: "Диагностика ещё не готова",
                    subtitle: store.errorMessage ?? "Первый расчёт появится после обновления часовой аналитики.",
                    systemImage: "chart.xyaxis.line"
                )
                .padding()
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("Качество торговли")
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

    @ViewBuilder
    private func overviewContent(payload: DashboardPayload, quality: TradeQualityPayload) -> some View {
        let overview = quality.overview
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(
                    title: "Часовой разворот · результат за \(quality.periodDays ?? 30) дней",
                    subtitle: "Расчёт по часовым свечам; минутные данные используются только на границах сделки."
                )
                if (overview?.closedTrades ?? 0) == 0 {
                    Text("Закрытых сделок текущей стратегии пока нет. Результаты появятся после первого закрытия.")
                        .font(.subheadline).foregroundStyle(.secondary)
                } else {
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                    MetricGlassTile(title: "После комиссий", value: formatRub(overview?.netPnlRub), tone: statusTone(for: overview?.netPnlRub))
                    MetricGlassTile(title: "Прибыльных", value: formatPct(overview?.winRatePct))
                    MetricGlassTile(title: "Удержали прибыли", value: formatPct(overview?.profitCapturePct))
                    MetricGlassTile(title: "Комиссии", value: formatRub(overview?.commissionRub), tone: .orange)
                    MetricGlassTile(title: "Ранние выходы", value: "\(overview?.materialEarlyExitCount ?? 0)")
                    MetricGlassTile(
                        title: "Проверка удержаний",
                        value: "\(overview?.strategyHypothesesPositiveCount ?? 0) из \(overview?.strategyHypothesesEvaluatedCount ?? 0)"
                    )
                    }
                }
            }
        }

        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "По инструментам", subtitle: "Фактический результат и качество удержания движения.")
                if quality.bySymbol.isEmpty {
                    Text("Закрытых сделок по инструментам пока нет.")
                        .font(.subheadline).foregroundStyle(.secondary)
                }
                ForEach(quality.bySymbol) { item in
                    VStack(alignment: .leading, spacing: 8) {
                        HStack {
                            Text(instrumentName(item.symbol, payload: payload))
                                .font(.headline)
                            Spacer()
                            Text(formatRub(item.netPnlRub))
                                .font(.headline.monospacedDigit())
                                .foregroundStyle(statusTone(for: item.netPnlRub))
                        }
                        InfoRow(title: "Сделки", value: "\(item.trades ?? 0) · в плюс \(formatPct(item.winRatePct))")
                        InfoRow(title: "Удержали", value: formatPct(item.profitCapturePct))
                        InfoRow(title: "Потенциал / просадка", value: "\(formatPct(item.averageMfePct)) / \(formatPct(item.averageMaePct))")
                        if let count = item.earlyExitCount, count > 0 {
                            InfoRow(title: "Ранние выходы", value: "\(count) · затем \(formatPct(item.averageEarlyExit4hPct)) за 4ч", accent: .orange)
                        }
                    }
                    if item.id != quality.bySymbol.last?.id {
                        Divider().overlay(Color.white.opacity(0.08))
                    }
                }
            }
        }

        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "Подтверждённые пропуски", subtitle: "Как цена двигалась после неисполненного входа.")
                if quality.missedEntries.isEmpty {
                    Text("Подтверждённых пропусков пока нет.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(Array(quality.missedEntries.prefix(10))) { item in
                        VStack(alignment: .leading, spacing: 7) {
                            HStack {
                                Text(instrumentName(item.symbol, payload: payload)).font(.headline)
                                Spacer()
                                SignalPill(text: displaySignal(item.signal), raw: item.signal)
                            }
                            Text(item.sourceLabel ?? "Вход не исполнен")
                                .font(.subheadline.weight(.semibold))
                            Text("1ч \(formatPct(item.move1hPct)) · 2ч \(formatPct(item.move2hPct)) · 4ч \(formatPct(item.move4hPct)) · 8ч \(formatPct(item.move8hPct))")
                                .font(.caption.monospacedDigit())
                                .foregroundStyle(.secondary)
                            Text(item.reason ?? "Причина не сохранена")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Divider().overlay(Color.white.opacity(0.08))
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func tradesContent(payload: DashboardPayload, quality: TradeQualityPayload) -> some View {
        let trades = quality.trades.sorted { ($0.exitTime ?? "") > ($1.exitTime ?? "") }
        if trades.isEmpty {
            EmptyGlassState(title: "Сделок для сравнения нет", subtitle: "История появится после закрытых позиций.", systemImage: "list.bullet.rectangle")
        } else {
            ForEach(Array(trades.prefix(20))) { trade in
                GlassCard {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack(alignment: .top) {
                            VStack(alignment: .leading, spacing: 3) {
                                Text(instrumentName(trade.symbol, payload: payload)).font(.headline)
                                Text(trade.exitTime ?? "-").font(.caption).foregroundStyle(.secondary)
                            }
                            Spacer()
                            SignalPill(text: displaySignal(trade.side), raw: trade.side)
                        }
                        InfoRow(title: "Фактический итог", value: formatRub(trade.pnlRub), accent: statusTone(for: trade.pnlRub))
                        InfoRow(title: "Максимум по ходу", value: formatRub(trade.maxPossibleNetRub))
                        InfoRow(title: "Недобрано", value: formatRub(trade.missedProfitRub), accent: .orange)
                        Divider().overlay(Color.white.opacity(0.08))
                        holdRow(hours: 1, value: trade.hold1hNetRub, delta: trade.hold1hDeltaRub)
                        holdRow(hours: 2, value: trade.hold2hNetRub, delta: trade.hold2hDeltaRub)
                        holdRow(hours: 4, value: trade.hold4hNetRub, delta: trade.hold4hDeltaRub)
                        holdRow(hours: 8, value: trade.hold8hNetRub, delta: trade.hold8hDeltaRub)
                        Text(trade.exitReason ?? "Причина выхода не сохранена")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func exitsContent(payload: DashboardPayload, quality: TradeQualityPayload) -> some View {
        let exits = (quality.exitDiagnostics ?? []).filter { $0.isMaterialEarlyExit == true }
        if exits.isEmpty {
            EmptyGlassState(title: "Ранних выходов пока нет", subtitle: "Появятся после закрытия и проверки следующего движения цены.", systemImage: "arrow.uturn.backward")
        } else {
            ForEach(Array(exits.prefix(12))) { item in
                GlassCard {
                    VStack(alignment: .leading, spacing: 8) {
                        Text(instrumentName(item.symbol, payload: payload)).font(.headline)
                        InfoRow(title: "После выхода за 4ч", value: formatPct(item.postExit4hPct))
                        Text(item.exitReason ?? "Причина выхода не сохранена").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func hypothesesContent(payload: DashboardPayload, quality: TradeQualityPayload) -> some View {
        let rows = quality.strategyHypotheses ?? []
        if rows.isEmpty {
            EmptyGlassState(title: "Гипотез пока нет", subtitle: "Здесь появятся проверенные движения после сигнала без входа.", systemImage: "chart.line.uptrend.xyaxis")
        } else {
            ForEach(Array(rows.prefix(12))) { item in
                GlassCard {
                    VStack(alignment: .leading, spacing: 8) {
                        Text(instrumentName(item.symbol, payload: payload)).font(.headline)
                        InfoRow(title: "Лучшее движение за 4ч", value: formatPct(item.bestMove4hPct))
                        Text(item.reason ?? "Причина не сохранена").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
    }

    private func holdRow(hours: Int, value: Double?, delta: Double?) -> some View {
        InfoRow(
            title: "Если держать \(hours)ч",
            value: value == nil ? "ждём данные" : "\(formatRub(value)) · к факту \(formatRub(delta))",
            accent: statusTone(for: delta)
        )
    }

    private func instrumentName(_ symbol: String, payload: DashboardPayload) -> String {
        guard let name = payload.instrumentCatalog?[symbol], !name.isEmpty else { return symbol }
        return "\(symbol) — \(name)"
    }
}
