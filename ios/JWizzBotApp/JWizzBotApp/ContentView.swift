import SwiftUI

struct ContentView: View {
    @State private var store = DashboardStore()

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

            TradesScreen(store: store)
                .tabItem {
                    Label("Сделки", systemImage: "list.bullet.rectangle")
                }

            NewsScreen(store: store)
                .tabItem {
                    Label("Новости", systemImage: "newspaper")
                }
        }
        .tint(Color.cyan)
        .task {
            await store.load()
        }
    }
}

#Preview {
    ContentView()
}
