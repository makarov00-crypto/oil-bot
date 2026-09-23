import SwiftUI

struct ShadowStrategyScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        Group {
            if let research = store.shadowStrategyPayload {
                ScreenContainer {
                    exitSection(research.exitExperiment)
                }
                .refreshable { await store.loadShadowStrategy() }
            } else if let error = store.shadowStrategyErrorMessage {
                EmptyGlassState(
                    title: "Данные не загрузились",
                    subtitle: error,
                    systemImage: "exclamationmark.triangle"
                )
                .padding()
                .background(LiquidGlassBackground())
            } else {
                VStack(spacing: 14) {
                    ProgressView()
                    Text("Загружаю гипотезу выхода…")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("Гипотеза выхода")
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                Button {
                    Task { await store.loadShadowStrategy() }
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
            }
        }
        .task {
            if store.shadowStrategyPayload == nil {
                await store.loadShadowStrategy()
            }
        }
    }

    private func exitSection(_ experiment: StrategyExitExperiment) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionHeader(
                    title: "Гипотеза выхода",
                    subtitle: "Условное удержание ещё двух часов после истощения AO. Реальные выходы не меняются."
                )
                let count = experiment.evaluated ?? 0
                let target = experiment.readiness?.targetEvaluated ?? 20
                InfoRow(title: "Завершено", value: "\(count) из \(target)")
                InfoRow(title: "Разница к обычному выходу", value: rub(experiment.deltaRub1Lot))
                Text(experiment.readiness?.status == "ready_for_limited_trial"
                    ? "Условия первичной проверки выполнены; нужен отдельный разбор перед изменением правила."
                    : experiment.readiness?.status == "not_confirmed"
                    ? "Гипотеза не подтвердилась на текущей выборке."
                    : "Выборка пока мала для вывода.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func rub(_ value: Double?) -> String {
        guard let value else { return "—" }
        return String(format: "%+.2f ₽/лот", value)
    }
}
