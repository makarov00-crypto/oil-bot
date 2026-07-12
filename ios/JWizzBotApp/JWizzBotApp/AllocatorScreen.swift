import SwiftUI

struct AllocatorScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var leftSymbol = ""
    @State private var rightSymbol = ""

    var body: some View {
        NavigationStack {
            Group {
                if let workspace = store.allocatorPayload {
                    allocatorContent(workspace)
                } else if store.isLoading {
                    ZStack {
                        LiquidGlassBackground()
                        ProgressView("Загружаю аллокатор…")
                    }
                } else {
                    EmptyGlassState(
                        title: "Нет данных аллокатора",
                        subtitle: store.allocatorErrorMessage ?? "После следующего цикла появятся кандидаты и решения.",
                        systemImage: "slider.horizontal.3"
                    )
                    .padding()
                    .background(LiquidGlassBackground())
                }
            }
            .navigationTitle("Аллокатор")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        Task { await store.loadAllocator() }
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func allocatorContent(_ workspace: AllocatorWorkspace) -> some View {
        ScreenContainer {
            SectionHeader(title: "Аллокатор", subtitle: workspace.generatedAtMoscow.map { "Обновлено: \($0)" })

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                MetricGlassTile(title: "Кандидатов", value: "\(workspace.summary.candidates)")
                MetricGlassTile(title: "Выбрано", value: "\(workspace.summary.selected)", tone: .green)
                MetricGlassTile(title: "Отложено", value: "\(workspace.summary.deferred)", tone: .orange)
                MetricGlassTile(title: "До рейтинга", value: "\(workspace.summary.blocked)", tone: workspace.summary.blocked > 0 ? .red : .white)
                MetricGlassTile(title: "Не хватает ГО", value: "\(workspace.summary.capitalBlocked)", tone: workspace.summary.capitalBlocked > 0 ? .red : .white)
                MetricGlassTile(title: "Свободно", value: formatRub(workspace.summary.freeCashRub))
            }

            GlassCard {
                VStack(alignment: .leading, spacing: 10) {
                    Text("Капитал сейчас")
                        .font(.headline)
                    InfoRow(title: "Занято под ГО", value: formatRub(workspace.summary.blockedGuaranteeRub))
                    InfoRow(title: "Открытых позиций", value: "\(workspace.summary.openPositions)")
                }
            }

            if workspace.candidates.isEmpty {
                EmptyGlassState(
                    title: "Сегодня кандидатов нет",
                    subtitle: "Когда появится сигнал на вход, здесь будет видно решение и его причину.",
                    systemImage: "waveform.path.ecg"
                )
            } else {
                ForEach(workspace.candidates) { candidate in
                    candidateCard(candidate)
                }
            }

            comparisonCard(workspace)
            decisionsCard(workspace)
        }
        .refreshable { await store.loadAllocator() }
        .onAppear { synchronizeComparison(workspace.candidates) }
        .onChange(of: workspace.generatedAtMoscow) { _, _ in
            synchronizeComparison(workspace.candidates)
        }
    }

    private func candidateCard(_ candidate: AllocatorCandidate) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("\(candidate.symbol) · \(displaySignal(candidate.signal))")
                            .font(.headline)
                        Text(candidate.displayName ?? candidate.timeDisplay ?? "-")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    SignalPill(text: candidate.decisionDisplay.uppercased(), raw: candidate.decision == "selected" ? "LONG" : candidate.decision == "blocked" ? "BLOCK" : "HOLD")
                }

                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 8) {
                    compactMetric("Приоритет", String(format: "%.2f", candidate.priorityScore))
                    compactMetric("Качество входа", String(format: "%.2f", candidate.entryEdgeScore))
                    compactMetric("Нужно ГО", candidate.requestedMarginRub > 0 ? formatRub(candidate.requestedMarginRub) : "не рассчитано")
                    compactMetric("Доступно", candidate.allocatableMarginRub > 0 ? formatRub(candidate.allocatableMarginRub) : "-")
                }

                if !candidate.priorityComponents.isEmpty {
                    VStack(alignment: .leading, spacing: 7) {
                        Text("Что повлияло")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 6) {
                            ForEach(candidate.priorityComponents.keys.sorted(), id: \.self) { key in
                                let value = candidate.priorityComponents[key] ?? 0
                                Text("\(key) \(value >= 0 ? "+" : "")\(String(format: "%.2f", value))")
                                    .font(.caption2.weight(.semibold))
                                    .foregroundStyle(value >= 0 ? .green : .red)
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 5)
                                    .background((value >= 0 ? Color.green : Color.red).opacity(0.12), in: Capsule())
                            }
                        }
                    }
                }

                DisclosureGroup("Причина и детали") {
                    VStack(alignment: .leading, spacing: 8) {
                        InfoRow(title: "Главная причина", value: humanizeAllocatorText(candidate.reason))
                        InfoRow(title: "Размер", value: candidate.quantity > 0 ? "\(candidate.quantity) лот(а)" : "не сохранён")
                        InfoRow(title: "Исполнение", value: candidate.executionNote ?? candidate.executionStatus ?? "не применимо")
                        InfoRow(title: "Проверка", value: candidate.outcome ?? "ожидает")
                    }
                    .padding(.top, 8)
                }
                .font(.subheadline.weight(.semibold))
            }
        }
    }

    @ViewBuilder
    private func comparisonCard(_ workspace: AllocatorWorkspace) -> some View {
        if workspace.candidates.count >= 2 {
            GlassCard {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Сравнение сигналов")
                        .font(.headline)
                    Picker("Первый", selection: $leftSymbol) {
                        ForEach(workspace.candidates) { candidate in
                            Text("\(candidate.symbol) · \(displaySignal(candidate.signal))")
                                .tag(candidate.symbol)
                        }
                    }
                    .pickerStyle(.menu)
                    Picker("Второй", selection: $rightSymbol) {
                        ForEach(workspace.candidates) { candidate in
                            Text("\(candidate.symbol) · \(displaySignal(candidate.signal))")
                                .tag(candidate.symbol)
                        }
                    }
                    .pickerStyle(.menu)

                    if let left = workspace.candidates.first(where: { $0.symbol == leftSymbol }),
                       let right = workspace.candidates.first(where: { $0.symbol == rightSymbol }),
                       left.symbol != right.symbol {
                        comparisonRow("Приоритет", left.priorityScore, right.priorityScore, left.symbol, right.symbol)
                        comparisonRow("Качество входа", left.entryEdgeScore, right.entryEdgeScore, left.symbol, right.symbol)
                        comparisonRow("Новости", left.priorityComponents["новости"] ?? 0, right.priorityComponents["новости"] ?? 0, left.symbol, right.symbol)
                        comparisonRow("Обучение", left.priorityComponents["обучение"] ?? 0, right.priorityComponents["обучение"] ?? 0, left.symbol, right.symbol)
                    }
                }
            }
        }
    }

    private func decisionsCard(_ workspace: AllocatorWorkspace) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("Журнал переключений")
                    .font(.headline)
                if workspace.recentDecisions.isEmpty {
                    Text("Сегодня переключений и отложенных решений не было.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(Array(workspace.recentDecisions.prefix(8))) { decision in
                        VStack(alignment: .leading, spacing: 4) {
                            Text("\(decision.decisionDisplay ?? "решение"): \(decision.symbol ?? "-") \(displaySignal(decision.signal))")
                                .font(.subheadline.weight(.semibold))
                            Text(allocatorDetails(decision))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if decision.id != workspace.recentDecisions.prefix(8).last?.id {
                            Divider().overlay(Color.white.opacity(0.08))
                        }
                    }
                }
            }
        }
    }

    private func compactMetric(_ title: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.subheadline.weight(.semibold)).lineLimit(2)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    private func comparisonRow(_ title: String, _ left: Double, _ right: Double, _ leftSymbol: String, _ rightSymbol: String) -> some View {
        let delta = left - right
        return InfoRow(
            title: title,
            value: "\(leftSymbol) \(String(format: "%.2f", left)) · \(rightSymbol) \(String(format: "%.2f", right)) · \(delta >= 0 ? "+" : "")\(String(format: "%.2f", delta))",
            accent: delta == 0 ? nil : (delta > 0 ? .green : .red)
        )
    }

    private func allocatorDetails(_ decision: AllocatorDecision) -> String {
        var parts: [String] = []
        if let priority = decision.priorityScore { parts.append("приоритет \(String(format: "%.2f", priority))") }
        if let margin = decision.requestedMarginRub { parts.append("ГО \(formatRub(margin))") }
        if let reason = decision.reason, !reason.isEmpty { parts.append(humanizeAllocatorText(reason)) }
        return parts.joined(separator: " · ")
    }

    private func synchronizeComparison(_ candidates: [AllocatorCandidate]) {
        guard candidates.count >= 2 else {
            leftSymbol = ""
            rightSymbol = ""
            return
        }
        let symbols = candidates.map(\.symbol)
        if !symbols.contains(leftSymbol) { leftSymbol = symbols[0] }
        if !symbols.contains(rightSymbol) || rightSymbol == leftSymbol { rightSymbol = symbols[1] }
    }
}
