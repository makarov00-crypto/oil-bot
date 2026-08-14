import SwiftUI

struct TradeQualityScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var segment = 0

    private let sections = ["Итог", "Сделки", "ИИ", "AO и Чайкин"]

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
                        shadowAIContent(payload.signalAIShadow)
                    } else {
                        aoChaikinContent(payload: payload, shadow: payload.aoChaikinShadow)
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
                    MetricGlassTile(title: "Пропущенные входы", value: "\(overview?.missedEntriesCount ?? 0)")
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
    private func shadowAIContent(_ shadow: SignalAIShadowPayload?) -> some View {
        if let shadow, shadow.enabled == true {
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(title: "Теневой ИИ", subtitle: "ИИ только оценивает сигналы и пока не управляет заявками.")
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                        MetricGlassTile(title: "Разобрано", value: "\(shadow.count ?? 0)")
                        MetricGlassTile(title: "Поддержал", value: "\(shadow.supporting ?? 0)", tone: .green)
                        MetricGlassTile(title: "Воздержался", value: "\(shadow.abstaining ?? 0)", tone: .orange)
                        MetricGlassTile(title: "Проверено 4ч", value: "\(evaluatedCount(shadow.reviews, horizon: "4h"))")
                    }
                }
            }

            ForEach(shadow.reviews) { item in
                GlassCard {
                    VStack(alignment: .leading, spacing: 9) {
                        HStack {
                            Text(item.symbol ?? "-").font(.headline)
                            Spacer()
                            SignalPill(text: item.review?.action ?? "НЕТ ОЦЕНКИ", raw: item.review?.direction)
                        }
                        InfoRow(title: "Сигнал", value: displaySignal(item.signal))
                        if let confidence = item.review?.confidence {
                            InfoRow(title: "Уверенность", value: formatPct(confidence * 100.0))
                        }
                        Text(item.review?.reason ?? "Причина не сохранена")
                            .font(.subheadline)
                        let outcomeText = ["1h", "2h", "4h", "8h"].compactMap { horizon -> String? in
                            guard let outcome = item.outcomes[horizon], let move = outcome.movePct else { return nil }
                            return "\(horizon.replacingOccurrences(of: "h", with: "ч")) \(formatPct(move))"
                        }.joined(separator: " · ")
                        if !outcomeText.isEmpty {
                            Text(outcomeText)
                                .font(.caption.monospacedDigit())
                                .foregroundStyle(.secondary)
                        }
                        if let risk = item.review?.riskNote, !risk.isEmpty {
                            Text("Риск: \(risk)")
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                    }
                }
            }
        } else {
            EmptyGlassState(title: "Теневой ИИ выключен", subtitle: "Рекомендации пока не собираются.", systemImage: "brain.head.profile")
        }
    }

    @ViewBuilder
    private func aoChaikinContent(payload: DashboardPayload, shadow: AOChaikinShadowPayload?) -> some View {
        if let shadow, shadow.enabled {
            let summary = shadow.summary
            GlassCard {
                VStack(alignment: .leading, spacing: 14) {
                    SectionHeader(
                        title: "Теневая стратегия AO и Чайкина",
                        subtitle: "Часовые свечи; только наблюдение, реальные заявки не меняются."
                    )
                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                        MetricGlassTile(title: "Проверок", value: "\(summary?.checks ?? 0)")
                        MetricGlassTile(title: "Входов", value: "\(summary?.entries ?? 0)", tone: .green)
                        MetricGlassTile(title: "Закрыто", value: "\(summary?.closedTrades ?? 0)")
                        MetricGlassTile(title: "Открыто", value: "\(summary?.openPositions ?? 0)", tone: .orange)
                        MetricGlassTile(title: "Итог на 1 лот", value: formatRub(summary?.netResultRub1Lot), tone: statusTone(for: summary?.netResultRub1Lot))
                        MetricGlassTile(title: "Прибыльных", value: formatPct(summary?.winRatePct))
                    }
                    InfoRow(title: "AO", value: shadow.settings?.aoPeriods ?? "5 и 34")
                    InfoRow(title: "Осциллятор Чайкина", value: shadow.settings?.chaikinPeriods ?? "5 и 20")
                    InfoRow(title: "Порог силы", value: formatPct(shadow.settings?.minimumStrengthPct))
                    InfoRow(title: "Правило выхода", value: shadow.settings?.exitRule ?? "три столбца против позиции")
                }
            }

            if !shadow.openPositions.isEmpty {
                GlassCard {
                    SectionHeader(title: "Открытые теневые позиции", subtitle: "Текущий расчёт на один лот.")
                }
                ForEach(shadow.openPositions.sorted { ($0.candleClosedAt ?? "") > ($1.candleClosedAt ?? "") }) { item in
                    aoChaikinEventCard(item, payload: payload)
                }
            }

            GlassCard {
                SectionHeader(title: "Последние решения", subtitle: "Сначала самые свежие закрытые часовые свечи.")
            }
            let decisions = shadow.decisions.sorted { ($0.candleClosedAt ?? "") > ($1.candleClosedAt ?? "") }
            if decisions.isEmpty {
                EmptyGlassState(
                    title: "Решений пока нет",
                    subtitle: "Первая запись появится после закрытия часовой свечи.",
                    systemImage: "waveform.path.ecg"
                )
            } else {
                ForEach(Array(decisions.prefix(24))) { item in
                    aoChaikinEventCard(item, payload: payload)
                }
            }

            if !shadow.closedTrades.isEmpty {
                GlassCard {
                    SectionHeader(title: "Закрытые теневые сделки", subtitle: "Результат на один лот после оценочной комиссии.")
                }
                ForEach(shadow.closedTrades.sorted { ($0.candleClosedAt ?? "") > ($1.candleClosedAt ?? "") }.prefix(20)) { item in
                    aoChaikinEventCard(item, payload: payload)
                }
            }
        } else {
            EmptyGlassState(
                title: "Теневая стратегия выключена",
                subtitle: "Расчёт AO и осциллятора Чайкина пока не собирается.",
                systemImage: "waveform.path.ecg"
            )
        }
    }

    private func aoChaikinEventCard(_ item: AOChaikinShadowEvent, payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 9) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 3) {
                        Text(instrumentName(item.symbol, payload: payload)).font(.headline)
                        Text(shadowEventTime(item.candleClosedAt)).font(.caption).foregroundStyle(.secondary)
                    }
                    Spacer()
                    SignalPill(text: item.decision ?? "НЕТ ВХОДА", raw: item.direction)
                }
                InfoRow(title: "Направление", value: (item.direction ?? "нет").lowercased())
                InfoRow(title: "Сила AO", value: formatPct(item.aoStrengthPct))
                InfoRow(title: "AO 5 и 34", value: String(format: "%.3f", item.ao ?? 0.0))
                InfoRow(title: "Осциллятор Чайкина", value: (item.chaikinStatus ?? "нейтрален").lowercased())
                InfoRow(title: "Ослабление движения", value: "\(item.oppositeAOBars ?? 0) из 3")
                if let result = item.estimatedNetRub1Lot {
                    InfoRow(title: "Результат на 1 лот", value: formatRub(result), accent: statusTone(for: result))
                }
                Text(item.reason ?? "Причина не сохранена")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if let entryTime = item.entryTime, !entryTime.isEmpty {
                    Text("Теневой вход: \(shadowEventTime(entryTime)) по цене \(formatPrice(item.entryPrice)).")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    private func shadowEventTime(_ raw: String?) -> String {
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

    private func holdRow(hours: Int, value: Double?, delta: Double?) -> some View {
        InfoRow(
            title: "Если держать \(hours)ч",
            value: value == nil ? "ждём данные" : "\(formatRub(value)) · к факту \(formatRub(delta))",
            accent: statusTone(for: delta)
        )
    }

    private func evaluatedCount(_ reviews: [SignalAIShadowReview], horizon: String) -> Int {
        reviews.filter { $0.outcomes[horizon] != nil }.count
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
