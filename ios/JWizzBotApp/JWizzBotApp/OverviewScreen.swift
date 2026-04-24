import SwiftUI

struct OverviewScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload {
                    ScreenContainer {
                        if let error = store.errorMessage {
                            inlineStatusCard(
                                title: "Последнее обновление с ошибкой",
                                message: error,
                                systemImage: "wifi.exclamationmark"
                            )
                        }

                        DateFilterBar(
                            dates: payload.daily.availableDates,
                            selectedDate: store.selectedDate
                        ) { newDate in
                            Task { await store.selectDate(newDate) }
                        }

                        if let capitalAlert = payload.capitalAlert, capitalAlert.active {
                            capitalAlertCard(capitalAlert)
                        }

                        headlineCard(payload: payload)
                        portfolioCard(payload: payload)
                        dailyCard(payload: payload)
                        pnlChartCard(payload: payload)
                        signalPulseCard(payload: payload)
                        runtimeCard(payload: payload)
                    }
                    .refreshable {
                        await store.load(date: store.selectedDate)
                    }
                } else if store.isLoading {
                    loadingView("Загружаю состояние бота…")
                } else {
                    EmptyGlassState(
                        title: "Нет данных",
                        subtitle: store.errorMessage ?? "Попробуй обновить позже.",
                        systemImage: "wifi.exclamationmark"
                    )
                    .padding()
                    .background(LiquidGlassBackground())
                }
            }
            .navigationTitle("Обзор")
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

    private func headlineCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text("Торговый день")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(payload.portfolio.selectedDateMoscow ?? displayDate(payload.daily.selectedDate))
                            .font(.title2.weight(.semibold))
                    }
                    Spacer()
                    SignalPill(text: displayMode(payload.portfolio.mode), raw: payload.portfolio.mode)
                }

                LazyVGrid(columns: twoColumns, spacing: 12) {
                    MetricGlassTile(title: "Итог бота", value: formatRub(payload.portfolio.botAnalyticalTotalPnlRub), tone: statusTone(for: payload.portfolio.botAnalyticalTotalPnlRub), help: portfolioHelp("analytical"))
                    MetricGlassTile(title: "Закрытые сделки", value: formatRub(payload.portfolio.botClosedNetPnlRub), tone: statusTone(for: payload.portfolio.botClosedNetPnlRub), help: portfolioHelp("closed"))
                    MetricGlassTile(title: "Открытые позиции", value: formatRub(payload.portfolio.botOpenPositionsLivePnlRub), tone: statusTone(for: payload.portfolio.botOpenPositionsLivePnlRub), help: portfolioHelp("open_live"))
                    MetricGlassTile(title: "Свободные деньги", value: formatRub(payload.portfolio.freeCashRub ?? payload.portfolio.freeRub), help: portfolioHelp("free"))
                    MetricGlassTile(title: "Открытых позиций", value: formatInt(payload.portfolio.openPositionsCount))
                }
            }
        }
    }

    private func portfolioCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                let subtitle = [payload.portfolio.generatedAtMoscow.map { "Срез портфеля: \($0)" }, payload.portfolio.selectedDateMoscow.map { "Дата отчёта: \($0)" }]
                    .compactMap { $0 }
                    .joined(separator: " | ")
                SectionHeader(title: "Портфель", subtitle: subtitle.isEmpty ? nil : subtitle)

                Text("Разделено по смыслу: деньги на счёте, результат сделок бота и сверка с брокерской вариационной маржей.")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                portfolioGroup(
                    title: "Счёт брокера",
                    subtitle: "Сколько денег есть и сколько уже занято под позиции.",
                    rows: [
                        PortfolioMetric("Стоимость портфеля", formatRub(payload.portfolio.totalPortfolioRub), .white, portfolioHelp("total")),
                        PortfolioMetric("Свободные деньги", formatRub(payload.portfolio.freeCashRub ?? payload.portfolio.freeRub), .white, portfolioHelp("free")),
                        PortfolioMetric("Занято под ГО", formatRub(payload.portfolio.blockedGuaranteeRub), .white, portfolioHelp("blocked")),
                        PortfolioMetric("Режим", displayMode(payload.portfolio.mode), .white, portfolioHelp("mode")),
                    ]
                )

                portfolioGroup(
                    title: "Результат бота",
                    subtitle: "То, что показывает торговая логика: закрытые сделки плюс текущий результат открытых позиций.",
                    rows: [
                        PortfolioMetric("Итог бота", formatRub(payload.portfolio.botAnalyticalTotalPnlRub), statusTone(for: payload.portfolio.botAnalyticalTotalPnlRub), portfolioHelp("analytical")),
                        PortfolioMetric("Закрытые сделки", formatRub(payload.portfolio.botClosedNetPnlRub), statusTone(for: payload.portfolio.botClosedNetPnlRub), portfolioHelp("closed")),
                        PortfolioMetric("Открытые позиции", formatRub(payload.portfolio.botOpenPositionsLivePnlRub), statusTone(for: payload.portfolio.botOpenPositionsLivePnlRub), portfolioHelp("open_live")),
                        PortfolioMetric("Gross закрытые + live", formatRub(payload.portfolio.botTotalVariationMarginRub), statusTone(for: payload.portfolio.botTotalVariationMarginRub), portfolioHelp("gross_live")),
                    ]
                )

                portfolioGroup(
                    title: "Сверка с брокером",
                    subtitle: "Брокерские движения по вариационной марже, комиссиям и денежному эффекту операций.",
                    rows: [
                        PortfolioMetric("Клиринговая ВМ", formatRub(payload.portfolio.botActualVarmarginRub), statusTone(for: payload.portfolio.botActualVarmarginRub), portfolioHelp("actual_vm")),
                        PortfolioMetric("Комиссия", formatRub(payload.portfolio.botActualFeeRub), statusTone(for: -(payload.portfolio.botActualFeeRub ?? 0)), portfolioHelp("fee")),
                        PortfolioMetric("Денежный эффект", formatRub(payload.portfolio.botOperationsCashEffectRub), statusTone(for: payload.portfolio.botOperationsCashEffectRub), portfolioHelp("cash_effect")),
                        PortfolioMetric("Текущая ВМ", formatRub(payload.portfolio.botEstimatedVariationMarginRub), statusTone(for: payload.portfolio.botEstimatedVariationMarginRub), portfolioHelp("estimated_vm")),
                    ]
                )
            }
        }
    }

    private struct PortfolioMetric: Identifiable {
        let id = UUID()
        let title: String
        let value: String
        let tone: Color
        let help: String

        init(_ title: String, _ value: String, _ tone: Color = .white, _ help: String) {
            self.title = title
            self.value = value
            self.tone = tone
            self.help = help
        }
    }

    private func portfolioGroup(title: String, subtitle: String, rows: [PortfolioMetric]) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            LazyVGrid(columns: twoColumns, spacing: 12) {
                ForEach(rows) { row in
                    MetricGlassTile(title: row.title, value: row.value, tone: row.tone, help: row.help)
                }
            }
        }
    }

    private func portfolioHelp(_ key: String) -> String {
        switch key {
        case "total":
            return "Оценка всего счёта у брокера: свободные деньги плюс текущая стоимость/результат открытых позиций по данным портфельного среза."
        case "free":
            return "Деньги, которые сейчас не заняты гарантийным обеспечением и могут использоваться для новых входов."
        case "blocked":
            return "Гарантийное обеспечение по открытым фьючерсным позициям. Чем выше эта сумма, тем меньше места для новых сделок."
        case "mode":
            return "Режим работы бота: боевой, тестовый, выходной или ожидание. Он влияет на разрешение новых входов."
        case "analytical":
            return "Главная цифра для оценки бота: закрытые сделки NET плюс текущий live-результат открытых позиций."
        case "closed":
            return "Финальный результат закрытых сделок бота после комиссий. Эта часть уже зафиксирована."
        case "open_live":
            return "Плавающий результат открытых позиций прямо сейчас. Он ещё может измениться до закрытия сделки."
        case "gross_live":
            return "Грубая сверка: результат закрытых сделок до части корректировок плюс live-результат открытых позиций."
        case "actual_vm":
            return "Вариационная маржа, которую брокер уже провёл клирингом по счёту за выбранный день."
        case "fee":
            return "Комиссии брокера по операциям счёта. В PnL бота они уменьшают итоговый результат."
        case "cash_effect":
            return "Денежный эффект операций по счёту: клиринговая вариационная маржа минус комиссии и связанные движения."
        case "estimated_vm":
            return "Расчётная текущая вариационная маржа по открытым позициям до следующего окончательного клиринга."
        default:
            return "Пояснение к показателю портфеля."
        }
    }

    private func dailyCard(payload: DashboardPayload) -> some View {
        let day = payload.daily.selected
        return GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "Дневная аналитика", subtitle: "Итог только за выбранную дату")

                LazyVGrid(columns: twoColumns, spacing: 12) {
                    MetricGlassTile(title: "Закрыто сделок", value: "\(day.closedCount)")
                    MetricGlassTile(title: "Win rate", value: winRateText(wins: day.wins, closed: day.closedCount))
                    MetricGlassTile(title: "Итог за день", value: formatRub(day.pnlRub), tone: statusTone(for: day.pnlRub))
                    MetricGlassTile(title: "Итог за день, %", value: formatPct(day.pnlPct), tone: statusTone(for: day.pnlPct))
                    MetricGlassTile(title: "Накоплено, RUB", value: formatRub(day.cumulativePnlRub), tone: statusTone(for: day.cumulativePnlRub))
                    MetricGlassTile(title: "Накоплено, %", value: formatPct(day.cumulativePnlPct), tone: statusTone(for: day.cumulativePnlPct))
                }
            }
        }
    }

    private func pnlChartCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "График PnL", subtitle: "Накопленный результат по дням")
                MiniPnlChart(series: payload.daily.series, selectedDate: payload.daily.selectedDate)
                HStack(spacing: 16) {
                    legendChip(color: .cyan, text: "Кривая PnL")
                    legendChip(color: .white, text: "Выбранная дата")
                }
            }
        }
    }

    private func signalPulseCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "Текущая картина", subtitle: payload.generatedAtMoscow.map { "Обновление: \($0)" })

                LazyVGrid(columns: threeColumns, spacing: 12) {
                    MetricGlassTile(title: "Лонг", value: "\(payload.summary.signalCounts?.long ?? 0)", tone: .green)
                    MetricGlassTile(title: "Шорт", value: "\(payload.summary.signalCounts?.short ?? 0)", tone: .red)
                    MetricGlassTile(title: "Ожидание", value: "\(payload.summary.signalCounts?.hold ?? 0)", tone: .orange)
                }
            }
        }
    }

    private func runtimeCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionHeader(title: "Мониторинг сервиса", subtitle: payload.runtime.updatedAtMoscow.map { "Статус runtime: \($0)" })

                InfoRow(title: "Состояние", value: displayRuntimeState(payload.runtime.state))
                InfoRow(title: "Сессия", value: displaySession(payload.runtime.session))
                InfoRow(title: "Последний цикл", value: payload.runtime.lastCycleAtMoscow ?? "-")
                InfoRow(title: "Циклов прошло", value: formatInt(payload.runtime.cycleCount))
                InfoRow(title: "Ошибок подряд", value: formatInt(payload.runtime.consecutiveErrors))
                InfoRow(title: "Старт сервиса", value: payload.runtime.startedAtMoscow ?? "-")
                InfoRow(title: "Health", value: payload.health?.ok == true ? "OK" : "ПРОВЕРИТЬ", accent: payload.health?.ok == true ? .green : .orange)
                InfoRow(title: "Oil Bot", value: payload.health?.botService?.active ?? "-")
                InfoRow(title: "Dashboard", value: payload.health?.dashboardService?.active ?? "-")
                InfoRow(title: "AI-разбор", value: payload.aiReview.available ? "ГОТОВ" : "НЕТ", accent: payload.aiReview.available ? .green : .orange)
                if let updated = payload.aiReview.updatedAtMoscow, !updated.isEmpty {
                    InfoRow(title: "AI обновлен", value: updated)
                }
                if let lastError = payload.runtime.lastError, !lastError.isEmpty {
                    InfoRow(title: "Последняя ошибка", value: lastError, accent: .orange)
                }
            }
        }
    }

    private func loadingView(_ text: String) -> some View {
        ZStack {
            LiquidGlassBackground()
            VStack(spacing: 14) {
                ProgressView()
                    .controlSize(.large)
                Text(text)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private func inlineStatusCard(title: String, message: String, systemImage: String) -> some View {
        GlassCard {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: systemImage)
                    .foregroundStyle(.orange)
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.subheadline.weight(.semibold))
                    Text(message)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    private func capitalAlertCard(_ alert: CapitalAlert) -> some View {
        GlassCard {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .foregroundStyle(.orange)
                    .font(.title3)
                VStack(alignment: .leading, spacing: 6) {
                    Text(alert.title ?? "Не хватает капитала для части сделок")
                        .font(.subheadline.weight(.semibold))
                    Text(alert.message ?? "Бот упёрся в ограничение по капиталу или ГО.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if let symbols = alert.symbols, !symbols.isEmpty {
                        Text("Инструменты: \(symbols.joined(separator: ", "))")
                            .font(.caption2)
                            .foregroundStyle(.orange)
                    }
                }
            }
        }
    }

    private func legendChip(color: Color, text: String) -> some View {
        HStack(spacing: 8) {
            Circle()
                .fill(color)
                .frame(width: 10, height: 10)
            Text(text)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func winRateText(wins: Int, closed: Int) -> String {
        guard closed > 0 else { return "0.0%" }
        let rate = Double(wins) / Double(closed) * 100
        return String(format: "%.1f%%", rate)
    }

    private var twoColumns: [GridItem] {
        [GridItem(.flexible()), GridItem(.flexible())]
    }

    private var threeColumns: [GridItem] {
        [GridItem(.flexible()), GridItem(.flexible()), GridItem(.flexible())]
    }
}
