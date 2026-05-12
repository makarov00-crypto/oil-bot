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
                                            Text(item.summary ?? item.source)
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
                                    if let category = item.category, !category.isEmpty {
                                        InfoRow(title: "Тема", value: category)
                                    }
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

    private func whyImportant(for item: NewsBiasItem) -> String {
        let pieces: [String] = [
            item.category,
            item.topics?.isEmpty == false ? "темы: \((item.topics ?? []).joined(separator: ", "))" : nil,
            item.reason
        ].compactMap { value in
            guard let value, !value.isEmpty else { return nil }
            return value
        }
        return pieces.joined(separator: " · ")
    }
}
