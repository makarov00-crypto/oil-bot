import SwiftUI

struct PositionsScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload, !payload.summary.openPositions.isEmpty {
                    ScreenContainer {
                        if let error = store.errorMessage {
                            GlassCard {
                                Label(error, systemImage: "wifi.exclamationmark")
                                    .font(.subheadline)
                                    .foregroundStyle(.orange)
                            }
                        }

                        ForEach(payload.summary.openPositions) { position in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            Text(position.symbol)
                                                .font(.title3.weight(.semibold))
                                            Text(position.strategy)
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        SignalPill(text: displaySignal(position.side), raw: position.side)
                                    }

                                    LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                                        compactInfo("Лоты", "\(position.qty)")
                                        compactInfo("Сигнал", displaySignal(position.lastSignal))
                                        compactInfo("Вход → текущая", "\(formatPrice(position.entryPrice)) → \(formatPrice(position.currentPrice))")
                                        compactInfo("Плавающий результат", "\(formatRub(position.variationMarginRub)) · \(formatPct(position.pnlPct))", tone: statusTone(for: position.variationMarginRub))
                                        compactInfo("Стоимость", formatRub(position.notionalRub))
                                        compactInfo("Стратегия", formatStrategyLabel(position.strategy))
                                    }
                                }
                            }
                        }
                    }
                    .refreshable { await store.load(date: store.selectedDate) }
                } else if store.isLoading {
                    ZStack {
                        LiquidGlassBackground()
                        ProgressView("Загружаю позиции…")
                    }
                } else {
                    EmptyGlassState(
                        title: "Открытых позиций нет",
                        subtitle: store.errorMessage ?? "Когда бот откроет сделку, она появится здесь.",
                        systemImage: "briefcase"
                    )
                    .padding()
                    .background(LiquidGlassBackground())
                }
            }
            .navigationTitle("Позиции")
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

    private func compactInfo(_ title: String, _ value: String, tone: Color = .white) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline)
                .foregroundStyle(tone)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
    }

    private func formatStrategyLabel(_ value: String) -> String {
        switch value {
        case "reversal_15m": return "15м разворот"
        case "reversal_1h": return "1ч разворот"
        case "momentum_breakout": return "Импульсный пробой"
        case "trend_pullback": return "Откат по тренду"
        case "trend_rollover": return "Перезапуск тренда"
        case "range_break_continuation": return "Продолжение пробоя диапазона"
        case "-", "": return "не определена"
        default: return value.replacingOccurrences(of: "_", with: " ")
        }
    }
}
