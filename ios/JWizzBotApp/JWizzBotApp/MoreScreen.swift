import SwiftUI

struct MoreScreen: View {
    @ObservedObject var store: DashboardStore

    var body: some View {
        NavigationStack {
            ScreenContainer {
                GlassCard {
                    VStack(alignment: .leading, spacing: 14) {
                        Text("Дополнительные разделы")
                            .font(.headline)
                        NavigationLink {
                            AllocatorScreen(store: store)
                        } label: {
                            menuRow(
                                title: "Аллокатор",
                                subtitle: "Кандидаты, причины выбора, ГО и сравнение сигналов",
                                systemImage: "slider.horizontal.3"
                            )
                        }
                        .buttonStyle(.plain)

                        Divider().overlay(Color.white.opacity(0.08))

                        NavigationLink {
                            NewsScreen(store: store)
                        } label: {
                            menuRow(
                                title: "Новости",
                                subtitle: "Активные сигналы, качество источников и влияние на входы",
                                systemImage: "newspaper"
                            )
                        }
                        .buttonStyle(.plain)

                        Divider().overlay(Color.white.opacity(0.08))

                        NavigationLink {
                            AIReviewScreen(store: store)
                        } label: {
                            menuRow(
                                title: "AI-разбор дня",
                                subtitle: "Ежедневная аналитика по сделкам и качеству стратегии",
                                systemImage: "brain.head.profile"
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
            .navigationTitle("Ещё")
        }
    }

    private func menuRow(title: String, subtitle: String, systemImage: String) -> some View {
        HStack(spacing: 14) {
            ZStack {
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(Color.white.opacity(0.08))
                    .frame(width: 44, height: 44)
                Image(systemName: systemImage)
                    .foregroundStyle(.cyan)
            }

            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            Image(systemName: "chevron.right")
                .font(.caption.weight(.bold))
                .foregroundStyle(.secondary)
        }
    }
}
