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
                                MetricGlassTile(title: "Активных bias", value: "\(payload.news.activeBiases.count)")
                                MetricGlassTile(title: "Выбрана дата", value: displayDate(store.selectedDate ?? payload.daily.selectedDate))
                            }
                        }
                    }

                    if payload.news.activeBiases.isEmpty {
                        EmptyGlassState(
                            title: "Активных новостей нет",
                            subtitle: "Когда news bias появятся, они будут видны здесь.",
                            systemImage: "newspaper"
                        )
                    } else {
                        ForEach(payload.news.activeBiases) { item in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            Text(item.symbol)
                                                .font(.title3.weight(.semibold))
                                            Text(item.source)
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        VStack(alignment: .trailing, spacing: 6) {
                                            SignalPill(text: displayBias(item.bias), raw: item.bias)
                                            SignalPill(text: displayBias(item.strength), raw: item.strength)
                                        }
                                    }

                                    Text(item.reason)
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
                    subtitle: store.errorMessage ?? "Активные news bias появятся здесь.",
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
}
