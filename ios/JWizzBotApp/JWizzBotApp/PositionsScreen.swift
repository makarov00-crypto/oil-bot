import SwiftUI

struct PositionsScreen: View {
    @Bindable var store: DashboardStore

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload, !payload.summary.openPositions.isEmpty {
                    List {
                        if let error = store.errorMessage {
                            errorBanner(error)
                                .listRowBackground(Color.clear)
                                .listRowInsets(EdgeInsets(top: 8, leading: 0, bottom: 4, trailing: 0))
                        }

                        ForEach(payload.summary.openPositions) { position in
                            VStack(alignment: .leading, spacing: 10) {
                                HStack {
                                    Text(position.symbol)
                                        .font(.headline)
                                    Spacer()
                                    Text(position.side)
                                        .font(.caption.weight(.bold))
                                        .padding(.horizontal, 10)
                                        .padding(.vertical, 5)
                                        .background(badgeColor(for: position.side).opacity(0.18), in: Capsule())
                                }
                                Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 8) {
                                    GridRow { label("Лоты"); value("\(position.qty)") }
                                    GridRow { label("Вход"); value(formatPrice(position.entryPrice)) }
                                    GridRow { label("Текущая"); value(formatPrice(position.currentPrice)) }
                                    GridRow { label("Стоимость"); value(formatRub(position.notionalRub)) }
                                    GridRow { label("Вар. маржа"); value(formatRub(position.variationMarginRub)) }
                                    GridRow { label("Изм. %"); value(formatPct(position.pnlPct)) }
                                    GridRow { label("Стратегия"); value(position.strategy) }
                                    GridRow { label("Сигнал"); value(position.lastSignal) }
                                }
                            }
                            .padding(.vertical, 6)
                        }
                    }
                } else if store.isLoading {
                    ProgressView("Загружаю позиции…")
                } else {
                    ContentUnavailableView(
                        "Открытых позиций нет",
                        systemImage: "briefcase",
                        description: Text(store.errorMessage ?? "Когда бот откроет сделку, она появится здесь.")
                    )
                }
            }
            .navigationTitle("Позиции")
            .refreshable {
                await store.load()
            }
        }
    }

    private func errorBanner(_ text: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: "wifi.exclamationmark")
                .foregroundStyle(.orange)
            Text(text)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.orange.opacity(0.08), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }

    private func label(_ text: String) -> some View {
        Text(text)
            .font(.caption)
            .foregroundStyle(.secondary)
    }

    private func value(_ text: String) -> some View {
        Text(text)
            .font(.subheadline)
    }
}
