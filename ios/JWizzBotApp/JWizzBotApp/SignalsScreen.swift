import SwiftUI

struct SignalsScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var newTicker = ""
    @State private var selectedTemplateSymbol = ""

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

    private var manualTemplates: [InstrumentTemplate] {
        store.payload?.manualInstruments?.templates ?? []
    }

    private var customInstruments: [CustomInstrumentItem] {
        store.payload?.manualInstruments?.customInstruments ?? []
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

                        manualInstrumentCard

                        ForEach(sortedStates, id: \.0) { symbol, state in
                            GlassCard {
                                VStack(alignment: .leading, spacing: 12) {
                                    HStack(alignment: .top) {
                                        VStack(alignment: .leading, spacing: 4) {
                                            instrumentTitle(symbol, explicitName: state.displayName)
                                            Text(state.strategyName ?? state.entryStrategy ?? "-")
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        SignalPill(text: displaySignal(state.lastSignal), raw: state.lastSignal)
                                    }

                                    HStack(spacing: 8) {
                                        SignalPill(text: displayScope(for: state), raw: scopeRaw(for: state))
                                        SignalPill(text: displayBias(state.newsBias), raw: state.newsBias)
                                    }

                                    Divider().overlay(Color.white.opacity(0.08))

                                    InfoRow(title: "Новости", value: newsSummary(for: state))
                                    if let allocator = state.lastAllocatorSummary, !allocator.isEmpty {
                                        InfoRow(title: "Аллокатор", value: allocatorSummary(allocator))
                                    }
                                    InfoRow(title: "Позиция", value: "\(displaySignal(state.positionSide)) / \(state.positionQty ?? 0) лот")
                                    InfoRow(title: "Почему сейчас", value: firstSummary(for: state))

                                    if state.signalSummary.count > 1 {
                                        VStack(alignment: .leading, spacing: 6) {
                                            Text("Подробности")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                            ForEach(state.signalSummary.dropFirst(), id: \.self) { line in
                                                Text("• \(shortSummary(line))")
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

    private func instrumentTitle(_ symbol: String, explicitName: String? = nil) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(symbol)
                .font(.title3.weight(.semibold))
            let name = instrumentName(symbol, explicitName: explicitName)
            if let name {
                Text(name)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func instrumentName(_ symbol: String, explicitName: String? = nil) -> String? {
        let name = (explicitName ?? store.payload?.instrumentCatalog?[symbol] ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty, name != symbol else { return nil }
        return name
    }

    private func firstSummary(for state: InstrumentSignalState) -> String {
        if let first = state.signalSummary.first, !first.isEmpty {
            return shortSummary(first)
        }
        return shortSummary(state.lastError ?? "-")
    }

    private func displayScope(for state: InstrumentSignalState) -> String {
        let strategy = (state.strategyName ?? state.entryStrategy ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if strategy == "reversal_15m" {
            return "АРХИВ 15М"
        }
        if strategy == "reversal_1h" {
            return "UNIFIED 1Ч"
        }
        return "LEGACY"
    }

    private func scopeRaw(for state: InstrumentSignalState) -> String? {
        let strategy = (state.strategyName ?? state.entryStrategy ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if strategy == "reversal_15m" || strategy == "reversal_1h" {
            return "UNIFIED"
        }
        return "LEGACY"
    }

    private func newsSummary(for state: InstrumentSignalState) -> String {
        let bias = displayBias(state.newsBias)
        let impact = (state.newsImpact ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !impact.isEmpty, impact != "-" else { return bias }
        return "\(bias) · \(shortSummary(impact, limit: 120))"
    }

    private func allocatorSummary(_ raw: String) -> String {
        shortSummary(raw, limit: 140)
    }

    private func shortSummary(_ raw: String, limit: Int = 160) -> String {
        let compact = raw
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .components(separatedBy: "•")
            .first?
            .components(separatedBy: "|")
            .first?
            .components(separatedBy: ". ")
            .first?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? raw
        guard compact.count > limit else { return compact }
        return String(compact.prefix(limit - 3)) + "..."
    }

    private var manualInstrumentCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionHeader(title: "Ручное добавление инструмента", subtitle: "Новый тикер унаследует стратегии выбранного шаблона")

                if let message = store.addInstrumentMessage, !message.isEmpty {
                    Text(message)
                        .font(.caption)
                        .foregroundStyle(message.contains("Не удалось") || message.contains("Сначала") || message.contains("ошиб") ? .orange : .secondary)
                }

                TextField("Новый тикер, например VBU6", text: $newTicker)
                    .textInputAutocapitalization(.characters)
                    .autocorrectionDisabled()
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
                    .background(.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

                if !manualTemplates.isEmpty {
                    Picker("Инструмент-шаблон", selection: Binding(
                        get: { validTemplateSymbol },
                        set: { selectedTemplateSymbol = $0 }
                    )) {
                        ForEach(manualTemplates, id: \.symbol) { template in
                            Text(templateLine(template))
                                .tag(template.symbol)
                        }
                    }
                    .pickerStyle(.menu)
                    .onAppear {
                        if selectedTemplateSymbol.isEmpty || !manualTemplates.contains(where: { $0.symbol == selectedTemplateSymbol }) {
                            selectedTemplateSymbol = manualTemplates.first?.symbol ?? ""
                        }
                    }
                }

                Button {
                    Task {
                        await store.addManualInstrument(symbol: newTicker, cloneFrom: validTemplateSymbol)
                        if store.addInstrumentMessage?.contains("добавлен") == true || store.addInstrumentMessage?.contains("обновл") == true {
                            newTicker = ""
                        }
                    }
                } label: {
                    HStack {
                        if store.isAddingInstrument {
                            ProgressView()
                                .controlSize(.small)
                        }
                        Text(store.isAddingInstrument ? "Добавляю…" : "Добавить инструмент")
                            .font(.subheadline.weight(.semibold))
                    }
                    .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .disabled(store.isAddingInstrument || validTemplateSymbol.isEmpty)

                if !customInstruments.isEmpty {
                    Divider().overlay(Color.white.opacity(0.08))
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Уже добавлены вручную")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        ForEach(customInstruments) { item in
                            HStack {
                                Text(item.symbol)
                                    .font(.subheadline.weight(.semibold))
                                Spacer()
                                Text("как \(item.cloneFrom)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        }
    }

    private func templateLine(_ template: InstrumentTemplate) -> String {
        let primary = template.primaryStrategies.joined(separator: ", ")
        return "\(template.symbol) → \(primary)"
    }

    private var validTemplateSymbol: String {
        if manualTemplates.contains(where: { $0.symbol == selectedTemplateSymbol }) {
            return selectedTemplateSymbol
        }
        return manualTemplates.first?.symbol ?? ""
    }
}
