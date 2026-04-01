import SwiftUI

struct AIReviewScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        Group {
            if let payload = store.payload {
                ScreenContainer {
                    DateFilterBar(dates: payload.daily.availableDates, selectedDate: store.selectedDate) { newDate in
                        Task { await store.selectDate(newDate) }
                    }

                    GlassCard {
                        VStack(alignment: .leading, spacing: 10) {
                            HStack {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text("AI-разбор дня")
                                        .font(.headline)
                                    Text(displayDate(payload.aiReview.date ?? payload.daily.selectedDate))
                                        .font(.subheadline)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                if payload.aiReview.available {
                                    SignalPill(text: "ГОТОВ", raw: "ACTIVE")
                                } else {
                                    SignalPill(text: "НЕТ", raw: "HOLD")
                                }
                            }

                            if let source = payload.aiReview.source {
                                Text("Источник: \(source)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            if let updated = payload.aiReview.updatedAtMoscow, !updated.isEmpty {
                                Text("Последнее обновление: \(updated)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            if let status = payload.aiReview.status, !status.isEmpty {
                                Text("Статус: \(displayAIReviewStatus(status))")
                                    .font(.caption)
                                    .foregroundStyle(payload.aiReview.available ? .green : .orange)
                            }
                        }
                    }

                    if payload.aiReview.available {
                        GlassCard {
                            Text(reviewAttributedText(payload.aiReview.content))
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .textSelection(.enabled)
                        }
                    } else {
                        EmptyGlassState(
                            title: "AI-разбор пока не найден",
                            subtitle: "Сначала запусти локально remote_ai_review.py и опубликуй результат на сервер.",
                            systemImage: "brain.head.profile"
                        )
                    }
                }
                .refreshable { await store.load(date: store.selectedDate) }
            } else if store.isLoading {
                ProgressView("Загружаю AI-разбор…")
            } else {
                EmptyGlassState(
                    title: "Нет данных AI-разбора",
                    subtitle: store.errorMessage ?? "После обновления сервера разбор появится здесь.",
                    systemImage: "brain.head.profile"
                )
                .padding()
                .background(LiquidGlassBackground())
            }
        }
        .navigationTitle("AI-разбор")
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
