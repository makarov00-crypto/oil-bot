import SwiftUI

struct NewsScreen: View {
    @Bindable var store: DashboardStore
    @State private var selectedText: String?

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload {
                    List {
                        Section("Сводка") {
                            row("Обновлено", payload.news.fetchedAtMoscow ?? "-")
                            row("Активных bias", "\(payload.news.activeBiases.count)")
                        }

                        Section("Активные новости") {
                            if payload.news.activeBiases.isEmpty {
                                Text("Активных новостных сигналов нет.")
                                    .foregroundStyle(.secondary)
                            } else {
                                ForEach(payload.news.activeBiases) { item in
                                    VStack(alignment: .leading, spacing: 8) {
                                        HStack {
                                            Text(item.symbol)
                                                .font(.headline)
                                            Spacer()
                                            Text(item.bias)
                                                .font(.caption.weight(.bold))
                                                .padding(.horizontal, 10)
                                                .padding(.vertical, 5)
                                                .background(badgeColor(for: item.bias).opacity(0.18), in: Capsule())
                                        }
                                        Text(item.reason)
                                            .font(.subheadline)
                                        if let text = item.messageText, !text.isEmpty {
                                            Button {
                                                selectedText = text
                                            } label: {
                                                Label("Показать текст новости", systemImage: "text.quote")
                                                    .font(.caption)
                                            }
                                            .buttonStyle(.plain)
                                            .foregroundStyle(.cyan)
                                        }
                                        Text("Источник: \(item.source)")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        Text("Актуально до: \(item.expiresAtMoscow ?? "-")")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }
                                    .padding(.vertical, 6)
                                }
                            }
                        }
                    }
                } else if store.isLoading {
                    ProgressView("Загружаю новости…")
                } else {
                    ContentUnavailableView(
                        "Нет данных по новостям",
                        systemImage: "newspaper",
                        description: Text(store.errorMessage ?? "Активные news bias появятся здесь.")
                    )
                }
            }
            .navigationTitle("Новости")
            .refreshable {
                await store.load()
            }
            .sheet(item: Binding(
                get: { selectedText.map(SelectableText.init(text:)) },
                set: { selectedText = $0?.text }
            )) { item in
                NavigationStack {
                    ScrollView {
                        Text(item.text)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding()
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
    }

    private struct SelectableText: Identifiable {
        let text: String
        var id: String { text }
    }

    private func row(_ title: String, _ value: String) -> some View {
        HStack {
            Text(title)
            Spacer()
            Text(value)
                .foregroundStyle(.secondary)
        }
    }
}
