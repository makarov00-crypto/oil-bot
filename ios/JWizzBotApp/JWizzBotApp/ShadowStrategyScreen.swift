import SwiftUI

struct ShadowStrategyScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        Group {
            if let research = store.shadowStrategyPayload {
                ScreenContainer {
                    executionSection(research.execution)
                    aiSection(research.ai)
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
                    Text("Загружаю исследования…")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("Исследования")
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

    private func executionSection(_ execution: StrategyExecutionResearch) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionHeader(
                    title: "Исполнение AO / Чайкин",
                    subtitle: "Сигналы после перехода на новую стратегию: от кандидата до подтверждённого входа."
                )
                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                    MetricGlassTile(title: "Кандидаты", value: "\(execution.candidates)")
                    MetricGlassTile(title: "Выбрано", value: "\(execution.selected)")
                    MetricGlassTile(title: "Вход подтверждён", value: "\(execution.confirmed)", tone: .green)
                    MetricGlassTile(title: "Без подтверждения", value: "\(execution.selectedUnconfirmed)", tone: execution.selectedUnconfirmed > 0 ? .orange : .white)
                }
                Text(execution.candidates == 0
                    ? "После перехода ещё не было нового кандидата AO."
                    : "Отложено аллокатором или ограничениями: \(execution.deferred).")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                ForEach(execution.recent.prefix(12)) { event in
                    Divider().overlay(Color.white.opacity(0.08))
                    VStack(alignment: .leading, spacing: 5) {
                        Text("\(event.symbol) · \(event.signal)")
                            .font(.subheadline.weight(.semibold))
                        Text("\(event.observedAt) · \(event.decision) · \(event.executionStatus.isEmpty ? "нет подтверждения" : event.executionStatus)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }

    private func aiSection(_ ai: StrategyAIResearch) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 16) {
                SectionHeader(
                    title: "Теневой ИИ",
                    subtitle: "«Вход» и «Пропустить» проверяются по направлению цены через 4 часа. Комиссия и стоп в этой метрике не учтены."
                )
                aiCohort("AO / Чайкин · текущая", ai.byStrategy["ao_chaikin_1h"])
                Divider().overlay(Color.white.opacity(0.08))
                aiCohort("Часовой разворот · архив", ai.byStrategy["reversal_1h"])
                Text("Архивные оценки нельзя переносить на AO. В новой стратегии ИИ пока только наблюдает за сигналами.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func aiCohort(_ title: String, _ cohort: StrategyAICohort?) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title).font(.headline)
            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                MetricGlassTile(title: "Проверено", value: "\(cohort?.evaluated ?? 0)")
                MetricGlassTile(title: "ИИ: вход", value: percentage(cohort?.enterCorrectPct))
                MetricGlassTile(title: "ИИ: пропустить", value: percentage(cohort?.abstainCorrectPct))
                MetricGlassTile(title: "Движение по сигналу", value: percentage(cohort?.marketFavorablePct))
            }
            InfoRow(title: "Вход подтверждён", value: "\(cohort?.enterCorrect ?? 0) из \(cohort?.enter ?? 0)")
            InfoRow(title: "Пропуск оправдан", value: "\(cohort?.abstainCorrect ?? 0) из \(cohort?.abstain ?? 0)")
            InfoRow(title: "Среднее движение при «Вход»", value: percentage(cohort?.enterAverageMovePct))
            InfoRow(title: "Среднее движение при «Пропустить»", value: percentage(cohort?.abstainAverageMovePct))
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

    private func percentage(_ value: Double?) -> String {
        guard let value else { return "—" }
        return String(format: "%.1f%%", value)
    }

    private func rub(_ value: Double?) -> String {
        guard let value else { return "—" }
        return String(format: "%+.2f ₽/лот", value)
    }
}
