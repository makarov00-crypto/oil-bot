import SwiftUI

struct OverviewScreen: View {
    @Bindable var store: DashboardStore

    var body: some View {
        NavigationStack {
            Group {
                if let payload = store.payload {
                    ScrollView {
                        VStack(spacing: 16) {
                            if let error = store.errorMessage {
                                inlineStatusCard(
                                    title: "Последнее обновление с ошибкой",
                                    message: error,
                                    systemImage: "wifi.exclamationmark"
                                )
                            }
                            summaryCard(payload: payload)
                            portfolioCard(payload: payload)
                            runtimeCard(payload: payload)
                        }
                        .padding()
                    }
                    .refreshable {
                        await store.load()
                    }
                } else if store.isLoading {
                    loadingView("Загружаю состояние бота…")
                } else {
                    ContentUnavailableView(
                        "Нет данных",
                        systemImage: "wifi.exclamationmark",
                        description: Text(store.errorMessage ?? "Попробуй обновить позже.")
                    )
                }
            }
            .navigationTitle("Обзор")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        Task { await store.load() }
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }

    private func summaryCard(payload: DashboardPayload) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Сводка")
                .font(.headline)
            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                metricTile("Реализовано", value: formatRub(payload.summary.realizedPnlRub))
                metricTile("Открыто", value: "\(payload.summary.openPositions.count)")
                metricTile("Инструментов", value: "\(payload.summary.symbolsTotal)")
                metricTile("Обновлено", value: payload.generatedAtMoscow ?? "-")
            }
        }
        .padding()
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }

    private func portfolioCard(payload: DashboardPayload) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Портфель")
                .font(.headline)
            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                metricTile("Режим", value: displayMode(payload.portfolio.mode))
                metricTile("Сессия", value: displaySession(payload.runtime.session))
                metricTile("Портфель", value: formatRub(payload.portfolio.totalPortfolioRub))
                metricTile("Свободно", value: formatRub(payload.portfolio.freeRub))
                metricTile("ГО", value: formatRub(payload.portfolio.blockedGuaranteeRub))
                metricTile("Итог", value: formatRub(payload.portfolio.botTotalPnlRub))
            }
        }
        .padding()
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }

    private func runtimeCard(payload: DashboardPayload) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Сервис")
                .font(.headline)
            infoRow("Runtime", payload.runtime.state ?? "-")
            infoRow("Последний цикл", payload.runtime.lastCycleAtMoscow ?? "-")
            infoRow("Срез портфеля", payload.portfolio.generatedAtMoscow ?? "-")
            infoRow("Обновлено в приложении", formatLoadTime(store.lastLoadedAt))
        }
        .padding()
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }

    private func loadingView(_ text: String) -> some View {
        VStack(spacing: 14) {
            ProgressView()
                .controlSize(.large)
            Text(text)
                .font(.subheadline)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private func inlineStatusCard(title: String, message: String, systemImage: String) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: systemImage)
                .foregroundStyle(.orange)
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                Text(message)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding()
        .background(Color.orange.opacity(0.08), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
    }

    private func metricTile(_ title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline.weight(.semibold))
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(Color.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
    }

    private func infoRow(_ title: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
