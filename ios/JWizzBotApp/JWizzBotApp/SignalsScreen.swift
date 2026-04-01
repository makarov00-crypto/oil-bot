import SwiftUI

struct SignalsScreen: View {
    @ObservedObject var store: DashboardStore

    private var sortedStates: [(String, InstrumentSignalState)] {
        guard let payload = store.payload else { return [] }
        return payload.states
            .sorted { lhs, rhs in
                let lhsPriority = priority(for: lhs.value.lastSignal)
                let rhsPriority = priority(for: rhs.value.lastSignal)
                if lhsPriority == rhsPriority {
                    return lhs.key < rhs.key
                }
                return lhsPriority < rhsPriority
            }
    }

    var body: some View {
        NavigationStack {
            Group {
                if store.payload != nil {
                    ScreenContainer {
                        if let error = store.errorMessage {
                            GlassCard {
                                Label(error, systemImage: "wifi.exclamationmark")
                                    .font(.subheadline)
                                    .foregroundStyle(.orange)
                            }
                        }

                        ForEach(sortedStates, id: \.0) { symbol, state in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            Text(symbol)
                                                .font(.title3.weight(.semibold))
                                            Text(state.strategyName ?? state.entryStrategy ?? "-")
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        SignalPill(text: displaySignal(state.lastSignal), raw: state.lastSignal)
                                    }

                                    HStack(spacing: 8) {
                                        SignalPill(text: "Старший ТФ: \(displaySignal(state.higherTFBias))", raw: state.higherTFBias)
                                        SignalPill(text: displayBias(state.newsBias), raw: state.newsBias)
                                    }

                                    Divider().overlay(Color.white.opacity(0.08))

                                    InfoRow(title: "Влияние новостей", value: state.newsImpact ?? "-")
                                    InfoRow(title: "Позиция", value: "\(displaySignal(state.positionSide)) / \(state.positionQty ?? 0) лот")
                                    InfoRow(title: "Ключевая причина", value: firstSummary(for: state))

                                    if state.signalSummary.count > 1 {
                                        VStack(alignment: .leading, spacing: 6) {
                                            Text("Детали")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                            ForEach(state.signalSummary.dropFirst(), id: \.self) { line in
                                                Text("• \(line)")
                                                    .font(.subheadline)
                                                    .foregroundStyle(.secondary)
                                            }
                                        }
                                    }

                                    if let error = state.lastError, !error.isEmpty {
                                        Divider().overlay(Color.white.opacity(0.08))
                                        Text(error)
                                            .font(.caption)
                                            .foregroundStyle(.orange)
                                    }
                                }
                            }
                        }
                    }
                    .refreshable { await store.load(date: store.selectedDate) }
                } else if store.isLoading {
                    ZStack {
                        LiquidGlassBackground()
                        ProgressView("Загружаю сигналы…")
                    }
                } else {
                    EmptyGlassState(
                        title: "Нет данных по сигналам",
                        subtitle: store.errorMessage ?? "Когда сервер отдаст свежий срез, сигналы появятся здесь.",
                        systemImage: "waveform.path.ecg"
                    )
                    .padding()
                    .background(LiquidGlassBackground())
                }
            }
            .navigationTitle("Сигналы")
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

    private func priority(for signal: String?) -> Int {
        switch (signal ?? "").uppercased() {
        case "LONG", "SHORT": return 0
        case "HOLD": return 1
        default: return 2
        }
    }

    private func firstSummary(for state: InstrumentSignalState) -> String {
        if let first = state.signalSummary.first, !first.isEmpty {
            return first
        }
        return state.lastError ?? "-"
    }
}
