import SwiftUI

struct TradeQualityScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0

    private let sections = ["Итог", "Сделки", "ИИ"]

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
                    } else {
                        shadowAIContent(payload.signalAIEntry)
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
                    title: "Результат за \(quality.periodDays ?? 30) дней",
                    subtitle: "Расчёт по часовым свечам; минутные данные используются только на границах сделки."
                )
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

        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "По инструментам", subtitle: "Фактический результат и качество удержания движения.")
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
                        if let action = trade.shadowAIAction, !action.isEmpty {
                            Divider().overlay(Color.white.opacity(0.08))
                            InfoRow(title: "Решение ИИ", value: aiAction(action))
                            if let confidence = trade.shadowAIConfidence {
                                InfoRow(title: "Уверенность", value: formatPct(confidence * 100.0))
                            }
                            if let reason = trade.shadowAIReason, !reason.isEmpty {
                                Text("Почему: \(reason)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            if let risk = trade.shadowAIRiskNote, !risk.isEmpty {
                                Text("Риск: \(risk)")
                                    .font(.caption)
                                    .foregroundStyle(.orange)
                            }
                        } else {
                            Divider().overlay(Color.white.opacity(0.08))
                            InfoRow(title: "Теневой ИИ", value: "Оценка не сохранялась")
                            Text("На момент этого входа оценка ИИ ещё не записывалась в историю сделки.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Text(trade.exitReason ?? "Причина выхода не сохранена")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func shadowAIContent(_ data: SignalAIEntryPayload?) -> some View {
        if let data {
            let c = data.counts
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(title: "ИИ на входах AO / Чайкин", subtitle: "Цепочка от кандидата до закрытой сделки. ИИ пока наблюдает и не меняет входы AO.")
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                        MetricGlassTile(title: "Кандидатов AO", value: "\(c.candidates)")
                        MetricGlassTile(title: "Ответов ИИ", value: "\(c.reviewed) / \(c.candidates)")
                        MetricGlassTile(title: "Вход подтверждён", value: "\(c.confirmed) / \(c.selected)")
                        MetricGlassTile(title: "Закрыто", value: "\(c.closed) / \(c.confirmed)")
                    }
                    if c.candidates == 0 {
                        Text("После перехода на AO кандидатов пока нет. Исторические ответы прежней стратегии исключены.")
                            .font(.caption).foregroundStyle(.secondary)
                    } else {
                        Text("\(c.unavailable) ответов ИИ не получено; \(c.unmatchedClosed) закрытых сделок не связаны с кандидатом.")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
            if c.candidates > 0 {
                GlassCard {
                    VStack(alignment: .leading, spacing: 12) {
                        SectionHeader(title: "Результат закрытых", subtitle: "NET по журналу брокера. Группы наблюдательные; эффект ИИ на торговлю не доказан.")
                        aiOutcomeRow("ИИ: вход", data.byAction["enter"])
                        aiOutcomeRow("ИИ: пропустить", data.byAction["abstain"])
                        Divider().overlay(Color.white.opacity(0.08))
                        InfoRow(title: "Диагностика цены 4ч", value: "\(c.priceChecked4h) проверено, \(c.priceCheckLate) поздних")
                        InfoRow(title: "Вход: цена по сигналу", value: "\(c.enterFavorable4h) из \(c.enterChecked4h)")
                        InfoRow(title: "Пропуск: цена против сигнала", value: "\(c.abstainUnfavorable4h) из \(c.abstainChecked4h)")
                        Text("Проверка 4ч не включает комиссию, стоп и размер позиции; она не показывает прибыль.")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }
                ForEach(data.recent) { item in
                    GlassCard {
                        VStack(alignment: .leading, spacing: 9) {
                            HStack {
                                Text(item.symbol ?? "—").font(.headline)
                                Spacer()
                                SignalPill(text: item.aiAction?.isEmpty == false ? item.aiAction! : "НЕТ ОТВЕТА", raw: item.signal)
                            }
                            InfoRow(title: "Сигнал", value: displaySignal(item.signal))
                            InfoRow(title: "Аллокатор", value: item.decision ?? "—")
                            InfoRow(title: "Исполнение", value: item.executionStatus?.isEmpty == false ? item.executionStatus! : "не подтверждено")
                            InfoRow(title: "NET", value: item.closedNetPnlRub.map { formatRub($0) } ?? "сделка не закрыта или не связана")
                            Text(item.aiReason?.isEmpty == false ? item.aiReason! : "Причина не записана")
                                .font(.caption).foregroundStyle(.secondary)
                            Text("\(item.model?.isEmpty == false ? item.model! : "модель не записана") · \(item.promptVersion?.isEmpty == false ? item.promptVersion! : "версия не записана")")
                                .font(.caption2).foregroundStyle(.tertiary)
                        }
                    }
                }
            }
        } else {
            EmptyGlassState(title: "Нет данных ИИ", subtitle: "Данные о кандидатах AO ещё не загружены.", systemImage: "brain.head.profile")
        }
    }

    @ViewBuilder
    private func aiOutcomeRow(_ title: String, _ group: SignalAIEntryGroup?) -> some View {
        if let group {
            InfoRow(title: title, value: "\(group.closed > 0 ? formatRub(group.netPnlRub) : "—") · \(group.closed) закрыто / \(group.executed) входов")
            if let averageR = group.averageR {
                InfoRow(title: "Средний R", value: String(format: "%.2f (%d сделок)", averageR, group.rEvaluated ?? 0))
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

    private func aiAction(_ raw: String) -> String {
        switch raw.uppercased() {
        case "ENTER": return "ВОЙТИ"
        case "HOLD": return "УДЕРЖИВАТЬ"
        case "EXIT": return "ВЫЙТИ"
        case "REVERSE": return "ПЕРЕВЕРНУТЬ"
        case "ABSTAIN": return "ВОЗДЕРЖАТЬСЯ"
        default: return raw
        }
    }

    private func instrumentName(_ symbol: String, payload: DashboardPayload) -> String {
        guard let name = payload.instrumentCatalog?[symbol], !name.isEmpty else { return symbol }
        return "\(symbol) — \(name)"
    }
}
