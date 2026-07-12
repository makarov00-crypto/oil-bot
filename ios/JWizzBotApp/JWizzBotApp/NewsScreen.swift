import SwiftUI

struct NewsScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var selectedText: String?

    var body: some View {
        Group {
            if let payload = store.payload {
                ScreenContainer {
                    GlassCard {
                        VStack(alignment: .leading, spacing: 14) {
                            SectionHeader(title: "Новости", subtitle: payload.news.fetchedAtMoscow.map { "Обновлено: \($0)" })

                            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                                MetricGlassTile(title: "Всего в работе", value: "\(payload.news.activeBiases.count)")
                                MetricGlassTile(title: "Срочно сейчас", value: "\(payload.news.activeBiases.filter { newsBucket(for: $0) == .key }.count)")
                                MetricGlassTile(title: "Влияет на сигналы", value: "\(payload.news.activeBiases.filter { newsBucket(for: $0) == .signal }.count)")
                                MetricGlassTile(title: "Жёсткий блок", value: "\(payload.news.activeBiases.filter { newsBucket(for: $0) == .block }.count)")
                            }
                        }
                    }

                    if let analytics = payload.news.analytics {
                        GlassCard {
                            VStack(alignment: .leading, spacing: 12) {
                                Text("Качество за \(analytics.days) дней")
                                    .font(.headline)
                                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 8) {
                                    compactMetric("Попадания", analytics.evaluatedCount > 0 ? "\(String(format: "%.1f", analytics.winRatePct))%" : "копится")
                                    compactMetric("Оценено", "\(analytics.evaluatedCount)")
                                    compactMetric("Ожидают", "\(analytics.pendingCount)")
                                    compactMetric("Недоступно", "\(analytics.unavailableCount)")
                                }
                                Text(analytics.evaluatedCount < 30
                                    ? "Выборка пока мала: источник не стоит считать лучшим до 20 наблюдений."
                                    : "Среднее движение после новости: \(String(format: "%.3f", analytics.avgMovePct))%.")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }

                        analyticsGroup(title: "Источники", rows: analytics.sources)
                        analyticsGroup(title: "Направление", rows: analytics.directions)
                        analyticsGroup(title: "Проверка ИИ", rows: analytics.aiConfirmation)
                    }

                    if let impact = payload.news.allocatorImpact {
                        GlassCard {
                            VStack(alignment: .leading, spacing: 10) {
                                Text("Влияние на аллокатор")
                                    .font(.headline)
                                InfoRow(title: "Изменили приоритет", value: "\(impact.totalCount)")
                                InfoRow(title: "Повысили", value: "\(impact.boostCount)", accent: .green)
                                InfoRow(title: "Понизили", value: "\(impact.penaltyCount)", accent: impact.penaltyCount > 0 ? .red : nil)
                                InfoRow(title: "Отложили вход", value: "\(impact.deferredCount)")
                                if impact.evaluatedSelectedCount > 0 {
                                    InfoRow(
                                        title: "Выбранные подтвердились",
                                        value: "\(impact.favorableSelectedCount)/\(impact.evaluatedSelectedCount) · \(String(format: "%.1f", impact.selectedWinRatePct))%"
                                    )
                                }
                            }
                        }
                    }

                    if payload.news.activeBiases.isEmpty {
                        EmptyGlassState(
                            title: "Активных новостей нет",
                            subtitle: "Когда появятся новые рыночные новости, они будут видны здесь.",
                            systemImage: "newspaper"
                        )
                    } else {
                        GlassCard {
                            VStack(alignment: .leading, spacing: 12) {
                                summaryRow(
                                    title: "Ключевое сейчас",
                                    items: payload.news.activeBiases.filter { newsBucket(for: $0) == .key },
                                    empty: "сильных новостей сейчас нет"
                                )
                                summaryRow(
                                    title: "Влияет на сигналы",
                                    items: payload.news.activeBiases.filter { newsBucket(for: $0) == .signal },
                                    empty: "новости не давят на сигналы"
                                )
                                summaryRow(
                                    title: "Фон",
                                    items: payload.news.activeBiases.filter { newsBucket(for: $0) == .background },
                                    empty: "фон спокоен"
                                )
                            }
                        }

                        ForEach(payload.news.activeBiases) { item in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            Text(item.symbol)
                                                .font(.title3.weight(.semibold))
                                            Text(item.summary ?? item.sourceLabel ?? item.source)
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        VStack(alignment: .trailing, spacing: 6) {
                                            SignalPill(text: displayBias(item.bias), raw: item.bias)
                                            SignalPill(text: displayNewsActionability(item.actionability), raw: item.bias)
                                        }
                                    }

                                    Text(whyImportant(for: item))
                                        .font(.subheadline)

                                    if let text = item.messageText, !text.isEmpty {
                                        Button {
                                            selectedText = text
                                        } label: {
                                            Label("Открыть текст новости", systemImage: "text.quote")
                                                .font(.subheadline.weight(.semibold))
                                                .foregroundStyle(.cyan)
                                        }
                                    }

                                    InfoRow(title: "Сейчас", value: "\(displayNewsHorizon(item.horizon)) · \(displayBias(item.strength))")
                                    if let aiReason = item.aiReason, !aiReason.isEmpty {
                                        InfoRow(title: "AI-разбор", value: aiSummary(for: item))
                                    }
                                    if let category = item.category, !category.isEmpty {
                                        InfoRow(title: "Тема", value: category)
                                    }
                                    InfoRow(title: "Источник", value: sourceSummary(for: item))
                                    if let expires = item.expiresAtMoscow, !expires.isEmpty {
                                        InfoRow(title: "Актуально до", value: expires)
                                    }
                                }
                            }
                        }
                    }
                }
                .refreshable { await store.load(date: store.selectedDate) }
            } else if store.isLoading {
                ZStack {
                    LiquidGlassBackground()
                    ProgressView("Загружаю новости…")
                }
            } else {
                EmptyGlassState(
                    title: "Нет данных по новостям",
                    subtitle: store.errorMessage ?? "Активные новости по инструментам появятся здесь.",
                    systemImage: "newspaper"
                )
                .padding()
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("Новости")
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                Button {
                    Task { await store.load(date: store.selectedDate) }
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
            }
        }
        .sheet(item: Binding(
            get: { selectedText.map(SelectableText.init(text:)) },
            set: { selectedText = $0?.text }
        )) { item in
            NavigationStack {
                ZStack {
                    LiquidGlassBackground()
                    ScrollView {
                        GlassCard {
                            Text(item.text)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .textSelection(.enabled)
                        }
                        .padding()
                    }
                }
                .navigationTitle("Текст новости")
                .navigationBarTitleDisplayMode(.inline)
                .toolbar {
                    ToolbarItem(placement: .topBarTrailing) {
                        Button("Закрыть") {
                            selectedText = nil
                        }
                    }
                }
            }
            .presentationDetents([.medium, .large])
        }
    }

    private struct SelectableText: Identifiable {
        let text: String
        var id: String { text }
    }

    private enum NewsBucket {
        case key
        case signal
        case block
        case background
    }

    @ViewBuilder
    private func summaryRow(title: String, items: [NewsBiasItem], empty: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.headline)
            Text(items.isEmpty ? empty : items.prefix(2).map { $0.summary ?? whyImportant(for: $0) }.joined(separator: " | "))
                .font(.subheadline)
                .foregroundStyle(.secondary)
        }
    }

    private func newsBucket(for item: NewsBiasItem) -> NewsBucket {
        switch (item.actionability ?? "").uppercased() {
        case "BLOCK": return .block
        case "ACTION": return .key
        case "WATCH": return .signal
        default:
            return (item.horizon ?? "").uppercased() == "NOW" ? .key : .background
        }
    }

    private func analyticsGroup(title: String, rows: [NewsAnalyticsRow]) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text(title)
                    .font(.headline)
                if rows.isEmpty {
                    Text("Данные ещё копятся.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(rows.prefix(4)) { row in
                        InfoRow(
                            title: row.label,
                            value: "\(String(format: "%.1f", row.winRatePct))% · \(row.totalCount) оценено"
                        )
                    }
                }
            }
        }
    }

    private func compactMetric(_ title: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.subheadline.weight(.semibold))
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    private func whyImportant(for item: NewsBiasItem) -> String {
        let pieces: [String] = [
            item.category,
            item.topics?.isEmpty == false ? "темы: \((item.topics ?? []).joined(separator: ", "))" : nil,
            item.aiReason?.isEmpty == false ? "AI: \(item.aiReason!)" : nil,
            item.aiRisk?.isEmpty == false ? "риск: \(item.aiRisk!)" : nil,
            item.reason
        ].compactMap { value in
            guard let value, !value.isEmpty else { return nil }
            return value
        }
        return pieces.joined(separator: " · ")
    }

    private func aiSummary(for item: NewsBiasItem) -> String {
        let direction = displayBias(item.aiDirection)
        let confidence = item.aiConfidence.map { "\(Int(($0 * 100).rounded()))%" } ?? "-"
        let reason = item.aiReason ?? "-"
        return "\(direction) · уверенность \(confidence) · \(reason)"
    }

    private func sourceSummary(for item: NewsBiasItem) -> String {
        let label = item.sourceLabel?.isEmpty == false ? item.sourceLabel! : item.source
        let typeMap = [
            "telegram": "быстрый Telegram",
            "broker_telegram": "Telegram брокера",
            "broker": "брокерская аналитика",
            "official": "официальный источник"
        ]
        let type = typeMap[(item.sourceType ?? "").lowercased()] ?? "источник"
        let speed = item.sourceSpeed.map { "\(Int(($0 * 100).rounded()))%" } ?? "-"
        let reliability = item.sourceReliability.map { "\(Int(($0 * 100).rounded()))%" } ?? "-"
        let confirmations = (item.confirmingSources ?? []).count > 1
            ? " · подтверждения: \((item.confirmingSources ?? []).joined(separator: ", "))"
            : ""
        return "\(label) · \(type) · скорость \(speed) · надёжность \(reliability)\(confirmations)"
    }
}
