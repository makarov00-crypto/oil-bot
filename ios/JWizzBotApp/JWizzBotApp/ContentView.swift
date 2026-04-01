import SwiftUI

struct ContentView: View {
    @StateObject private var store = DashboardStore()

    var body: some View {
        TabView {
            OverviewScreen(store: store)
                .tabItem {
                    Label("Обзор", systemImage: "gauge.medium")
                }

            PositionsScreen(store: store)
                .tabItem {
                    Label("Позиции", systemImage: "briefcase")
                }

            SignalsScreen(store: store)
                .tabItem {
                    Label("Сигналы", systemImage: "waveform.path.ecg")
                }

            TradesScreen(store: store)
                .tabItem {
                    Label("Сделки", systemImage: "list.bullet.rectangle")
                }

            MoreScreen(store: store)
                .tabItem {
                    Label("Ещё", systemImage: "ellipsis.circle")
                }
        }
        .preferredColorScheme(.dark)
        .tint(.cyan)
        .toolbarBackground(.ultraThinMaterial, for: .tabBar)
        .toolbarBackground(.visible, for: .tabBar)
        .task {
            await store.load()
        }
    }
}
