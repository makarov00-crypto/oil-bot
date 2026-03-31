import Foundation
import Observation

@MainActor
@Observable
final class DashboardStore {
    private(set) var payload: DashboardPayload?
    private(set) var isLoading = false
    private(set) var errorMessage: String?
    private(set) var lastLoadedAt: Date?

    private let dashboardURL = URL(string: "https://jwizzbot.ru/api/dashboard")!

    func load() async {
        if isLoading { return }
        isLoading = true
        defer { isLoading = false }

        do {
            let (data, response) = try await URLSession.shared.data(from: dashboardURL)
            guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
                throw URLError(.badServerResponse)
            }
            let decoder = JSONDecoder()
            let decoded = try decoder.decode(DashboardPayload.self, from: data)
            payload = decoded
            errorMessage = nil
            lastLoadedAt = Date()
        } catch {
            errorMessage = "Не удалось загрузить данные. Проверь соединение с сервером."
        }
    }
}
