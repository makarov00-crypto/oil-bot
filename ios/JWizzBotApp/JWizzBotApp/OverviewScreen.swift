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
                        Text(displayDate(payload.daily.selectedDate))
                            .font(.title2.weight(.semibold))
                    }
                    Spacer()
                    SignalPill(text: displayMode(payload.portfolio.mode), raw: payload.portfolio.mode)
                }

                LazyVGrid(columns: twoColumns, spacing: 12) {
                    MetricGlassTile(title: "Итог по боту", value: formatRub(payload.portfolio.botTotalPnlRub), tone: statusTone(for: payload.portfolio.botTotalPnlRub))
                    MetricGlassTile(title: "Реализовано ботом", value: formatRub(payload.portfolio.botRealizedPnlRub), tone: statusTone(for: payload.portfolio.botRealizedPnlRub))
                    MetricGlassTile(title: "Gross по сделкам", value: formatRub(payload.portfolio.botRealizedGrossPnlRub), tone: statusTone(for: payload.portfolio.botRealizedGrossPnlRub))
                    MetricGlassTile(title: "Комиссии", value: formatRub(payload.portfolio.botRealizedCommissionRub), tone: statusTone(for: -(payload.portfolio.botRealizedCommissionRub ?? 0)))
                    MetricGlassTile(title: "Факт. вар. маржа", value: formatRub(payload.portfolio.botActualVarmarginRub), tone: statusTone(for: payload.portfolio.botActualVarmarginRub))
                    MetricGlassTile(title: "Факт. эффект счёта", value: formatRub(payload.portfolio.botActualCashEffectRub), tone: statusTone(for: payload.portfolio.botActualCashEffectRub))
                    MetricGlassTile(title: "Вар. маржа", value: formatRub(payload.portfolio.botEstimatedVariationMarginRub), tone: statusTone(for: payload.portfolio.botEstimatedVariationMarginRub))
                    MetricGlassTile(title: "Открытых позиций", value: formatInt(payload.portfolio.openPositionsCount))
                }
            }
        }
    }

    private func portfolioCard(payload: DashboardPayload) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(title: "Портфель", subtitle: payload.portfolio.generatedAtMoscow.map { "Срез портфеля: \($0)" })

                LazyVGrid(columns: twoColumns, spacing: 12) {
                    MetricGlassTile(title: "Портфель", value: formatRub(payload.portfolio.totalPortfolioRub))
                    MetricGlassTile(title: "Свободно", value: formatRub(payload.portfolio.freeRub))
                    MetricGlassTile(title: "ГО", value: formatRub(payload.portfolio.blockedGuaranteeRub))
                    MetricGlassTile(title: "Режим", value: displayMode(payload.portfolio.mode))
                }
            }
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
