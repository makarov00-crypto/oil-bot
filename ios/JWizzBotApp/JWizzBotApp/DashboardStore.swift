import Foundation
import Combine

@MainActor
final class DashboardStore: ObservableObject {
    @Published private(set) var payload: DashboardPayload?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?
    @Published private(set) var lastLoadedAt: Date?
    @Published private(set) var selectedDate: String?

    private let dashboardURL = URL(string: "https://jwizzbot.ru/api/dashboard")!

    var availableDates: [String] {
        payload?.daily.availableDates ?? []
    }

    var selectedDailyPoint: DailyPoint? {
        payload?.daily.selected
    }

    func load(date: String? = nil) async {
        if isLoading { return }
        isLoading = true
        defer { isLoading = false }

        do {
            let requestURL = makeDashboardURL(date: date ?? selectedDate)
            let (data, response) = try await URLSession.shared.data(from: requestURL)
            guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
                throw URLError(.badServerResponse)
            }
            let decoder = JSONDecoder()
            let decoded = try decoder.decode(DashboardPayload.self, from: data)
            payload = decoded
            selectedDate = decoded.daily.selectedDate
            errorMessage = nil
            lastLoadedAt = Date()
        } catch {
            errorMessage = "Не удалось загрузить данные. Проверь соединение с сервером."
        }
    }

    func selectDate(_ date: String) async {
        await load(date: date)
    }

    private func makeDashboardURL(date: String?) -> URL {
        guard let date, !date.isEmpty else { return dashboardURL }
        var components = URLComponents(url: dashboardURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "date", value: date)]
        return components?.url ?? dashboardURL
    }
}
