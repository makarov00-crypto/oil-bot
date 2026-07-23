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
                    MetricGlassTile(title: "Счёт", value: formatRub(payload.portfolio.totalPortfolioRub), help: portfolioHelp("total"))
                    MetricGlassTile(title: "Свободные деньги", value: formatRub(calculatedFreeCash(payload.portfolio)), help: portfolioHelp("free"))
                    MetricGlassTile(title: "За сегодня", value: formatRub(payload.portfolio.botOpenPositionsIncomeRub), tone: statusTone(for: payload.portfolio.botOpenPositionsIncomeRub), help: portfolioHelp("open_income"))
                    MetricGlassTile(title: "Вар. маржа", value: formatRub(payload.portfolio.botEstimatedVariationMarginRub), tone: statusTone(for: payload.portfolio.botEstimatedVariationMarginRub), help: portfolioHelp("estimated_vm"))
                    MetricGlassTile(title: "Комиссии", value: formatRub(payload.portfolio.botActualFeeRub), tone: statusTone(for: -(payload.portfolio.botActualFeeRub ?? 0)), help: portfolioHelp("fee"))
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

                Text("Счёт брокера и три показателя текущего торгового дня.")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                portfolioGroup(
                    title: "Счёт брокера",
                    subtitle: "Оценка всего счёта и доступные для новых сделок деньги.",
                    rows: [
                        PortfolioMetric("Стоимость портфеля", formatRub(payload.portfolio.totalPortfolioRub), .white, portfolioHelp("total")),
                        PortfolioMetric("Свободные деньги", formatRub(calculatedFreeCash(payload.portfolio)), .white, portfolioHelp("free")),
                    ]
                )

                portfolioGroup(
                    title: "Сегодня",
                    subtitle: "Три независимых показателя из данных брокера.",
                    rows: [
                        PortfolioMetric("За сегодня", formatRub(payload.portfolio.botOpenPositionsIncomeRub), statusTone(for: payload.portfolio.botOpenPositionsIncomeRub), portfolioHelp("open_income")),
                        PortfolioMetric("Вар. маржа", formatRub(payload.portfolio.botEstimatedVariationMarginRub), statusTone(for: payload.portfolio.botEstimatedVariationMarginRub), portfolioHelp("estimated_vm")),
                        PortfolioMetric("Комиссии", formatRub(payload.portfolio.botActualFeeRub), statusTone(for: -(payload.portfolio.botActualFeeRub ?? 0)), portfolioHelp("fee")),
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

    private func calculatedFreeCash(_ portfolio: PortfolioSnapshot) -> Double? {
        if let value = portfolio.freeCashRub {
            return value
        }
        guard let total = portfolio.totalPortfolioRub else {
            return portfolio.freeRub
        }
        let blocked = portfolio.blockedGuaranteeRub ?? 0
        return max(0, total - blocked)
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
            return "Оценка всего счёта у брокера по портфельному срезу T-Invest. Формула у брокера: деньги + текущая оценка позиций и активов счёта."
        case "free":
            return "Расчётный свободный капитал после ГО. Формула: Стоимость портфеля − Занято под ГО."
        case "blocked":
            return "Гарантийное обеспечение по открытым фьючерсным позициям по данным брокера. Используется в формуле свободных денег: Стоимость портфеля − ГО."
        case "mode":
            return "Режим работы бота: боевой, тестовый, выходной или ожидание. Он влияет на разрешение новых входов."
        case "analytical":
            return "Главная цифра дня по боту. Формула: Валовый результат − Комиссия."
        case "closed":
            return "Зафиксированный результат закрытых сделок. Формула: сумма NET по CLOSE-сделкам журнала за выбранный день."
        case "open_income":
            return "Доход открытых позиций по полю expected_yield брокера. После вечернего клиринга он может отличаться от вариационной маржи."
        case "gross_live":
            return "Валовый результат до вычитания брокерских комиссий. Формула: gross PnL закрытых сделок + плавающий результат открытых позиций."
        case "actual_vm":
            return "Вариационная маржа, которую брокер уже провёл клирингом за выбранный день. Формула: сумма брокерских операций вариационной маржи."
        case "fee":
            return "Комиссии брокера по операциям счёта. Формула: сумма абсолютных значений fee-операций за выбранный день."
        case "cash_effect":
            return "Фактическое денежное движение по счету за день. Формула: Клиринговая ВМ + cash-effect комиссий, обычно это Клиринговая ВМ − Комиссия."
        case "estimated_vm":
            return "Текущая вариационная маржа по полю var_margin брокера. После вечернего клиринга она может отличаться от дохода открытых позиций."
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
                    MetricGlassTile(title: "Доля прибыльных", value: winRateText(wins: day.wins, closed: day.closedCount))
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
                SectionHeader(title: "Мониторинг сервиса", subtitle: payload.runtime.updatedAtMoscow.map { "Срез состояния: \($0)" })

                InfoRow(title: "Состояние", value: displayRuntimeState(payload.runtime.state))
                InfoRow(title: "Сессия", value: displaySession(payload.runtime.session))
                InfoRow(title: "Последний цикл", value: payload.runtime.lastCycleAtMoscow ?? "-")
                InfoRow(title: "Циклов прошло", value: formatInt(payload.runtime.cycleCount))
                InfoRow(title: "Ошибок подряд", value: formatInt(payload.runtime.consecutiveErrors))
                InfoRow(title: "Старт сервиса", value: payload.runtime.startedAtMoscow ?? "-")
                InfoRow(title: "Проверка", value: payload.health?.ok == true ? "НОРМА" : "ПРОВЕРИТЬ", accent: payload.health?.ok == true ? .green : .orange)
                InfoRow(title: "Бот", value: payload.health?.botService?.active ?? "-")
                InfoRow(title: "Панель", value: payload.health?.dashboardService?.active ?? "-")
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
