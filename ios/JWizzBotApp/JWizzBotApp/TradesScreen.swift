import SwiftUI

struct TradesScreen: View {
    @Bindable var store: DashboardStore

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload {
                    List {
                        Section("Сводка") {
                            row("Закрыто", "\(payload.tradeReview.closedCount)")
                            row("Плюсовых", "\(payload.tradeReview.wins)")
                            row("Минусовых", "\(payload.tradeReview.losses)")
                            row("Win rate", String(format: "%.1f%%", payload.tradeReview.winRate))
                            row("Итог", formatRub(payload.tradeReview.closedTotalPnlRub))
                        }

                        Section("Последние закрытия") {
                            if payload.tradeReview.closedReviews.isEmpty {
                                Text("Закрытых сделок пока нет.")
                                    .foregroundStyle(.secondary)
                            } else {
                                ForEach(payload.tradeReview.closedReviews.reversed()) { trade in
                                    VStack(alignment: .leading, spacing: 8) {
                                        HStack {
                                            Text(trade.symbol)
                                                .font(.headline)
                                            Spacer()
                                            Text(trade.pnlRub)
                                                .foregroundStyle((Double(trade.pnlRub) ?? 0) >= 0 ? .green : .red)
                                        }
                                        Text(trade.strategy)
                                            .font(.subheadline)
                                        Text("Вход: \(trade.entryTime)")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        Text("Выход: \(trade.exitTime)")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        Text(trade.exitReason)
                                            .font(.caption)
                                        Text("Вердикт: \(trade.verdict)")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }
                                    .padding(.vertical, 6)
                                }
                            }
                        }
                    }
                } else if store.isLoading {
                    ProgressView("Загружаю сделки…")
                } else {
                    ContentUnavailableView(
                        "Нет данных по сделкам",
                        systemImage: "list.bullet.rectangle",
                        description: Text(store.errorMessage ?? "Когда появятся закрытые сделки, они будут видны здесь.")
                    )
                }
            }
            .navigationTitle("Сделки")
            .refreshable {
                await store.load()
            }
        }
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
