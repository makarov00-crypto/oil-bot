import Foundation
import Combine

@MainActor
final class DashboardStore: ObservableObject {
    @Published private(set) var payload: DashboardPayload?
    @Published private(set) var isLoading = false
    @Published private(set) var errorMessage: String?
    @Published private(set) var lastLoadedAt: Date?
    @Published private(set) var selectedDate: String?
    @Published private(set) var isShowingCachedData = false

    private let dashboardURL = URL(string: "https://jwizzbot.ru/api/dashboard")!
    private let session: URLSession = {
        let config = URLSessionConfiguration.default
        config.waitsForConnectivity = true
        config.timeoutIntervalForRequest = 20
        config.timeoutIntervalForResource = 30
        config.requestCachePolicy = .reloadIgnoringLocalCacheData
        return URLSession(configuration: config)
    }()

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

        let targetDate = date ?? selectedDate
        do {
            let result = try await fetchDashboard(date: targetDate)
            let decoded = result.payload
            apply(decoded)
            do {
                try saveCache(result.data, for: targetDate)
            } catch {
                // Кэш — это удобство, а не причина ломать успешное обновление.
                print("Dashboard cache save warning:", error.localizedDescription)
            }
        } catch {
            if isCancellation(error) {
                return
            }
            if let cached = loadCache(for: targetDate) {
                apply(cached, cached: true)
                errorMessage = "Сервер временно недоступен. Показан сохранённый срез."
            } else {
                errorMessage = describe(error)
            }
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

    private func fetchDashboard(date: String?) async throws -> DashboardFetchResult {
        let requestURL = makeDashboardURL(date: date)
        var lastError: Error?
        for attempt in 0..<3 {
            do {
                let (data, response) = try await session.data(from: requestURL)
                guard let http = response as? HTTPURLResponse else {
                    throw DashboardLoadError.invalidResponse
                }
                guard (200..<300).contains(http.statusCode) else {
                    throw DashboardLoadError.httpStatus(http.statusCode)
                }
                do {
                    let decoded = try JSONDecoder().decode(DashboardPayload.self, from: data)
                    return DashboardFetchResult(payload: decoded, data: data)
                } catch {
                    throw DashboardLoadError.decoding(error.localizedDescription)
                }
            } catch {
                lastError = error
                if !shouldRetry(error) || attempt == 2 {
                    break
                }
                try? await Task.sleep(for: .milliseconds(500 * (attempt + 1)))
            }
        }
        throw lastError ?? DashboardLoadError.invalidResponse
    }

    private func shouldRetry(_ error: Error) -> Bool {
        if isCancellation(error) {
            return false
        }
        if let error = error as? DashboardLoadError {
            if case .httpStatus(let code) = error {
                return code >= 500
            }
            return false
        }
        guard let urlError = error as? URLError else { return false }
        switch urlError.code {
        case .timedOut, .cannotFindHost, .cannotConnectToHost, .networkConnectionLost, .notConnectedToInternet, .dnsLookupFailed:
            return true
        default:
            return false
        }
    }

    private func apply(_ decoded: DashboardPayload, cached: Bool = false) {
        payload = decoded
        selectedDate = decoded.daily.selectedDate
        errorMessage = nil
        lastLoadedAt = Date()
        isShowingCachedData = cached
    }

    private func saveCache(_ data: Data, for date: String?) throws {
        let url = cacheFileURL(for: date)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true,
            attributes: nil
        )
        try data.write(to: url, options: .atomic)
    }

    private func loadCache(for date: String?) -> DashboardPayload? {
        let url = cacheFileURL(for: date)
        guard
            let data = try? Data(contentsOf: url),
            let payload = try? JSONDecoder().decode(DashboardPayload.self, from: data)
        else {
            return nil
        }
        return payload
    }

    private func cacheFileURL(for date: String?) -> URL {
        let key = (date?.isEmpty == false ? date! : "latest").replacingOccurrences(of: "/", with: "-")
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
            ?? URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
        return base
            .appendingPathComponent("JWizzBotApp", isDirectory: true)
            .appendingPathComponent("dashboard-\(key).json")
    }

    private func isCancellation(_ error: Error) -> Bool {
        if error is CancellationError {
            return true
        }
        if let urlError = error as? URLError, urlError.code == .cancelled {
            return true
        }
        return false
    }

    private func describe(_ error: Error) -> String {
        if let error = error as? DashboardLoadError {
            switch error {
            case .httpStatus(let code):
                return "Сервер вернул ошибку \(code). Попробуй обновить ещё раз."
            case .invalidResponse:
                return "Сервер вернул неполный ответ. Попробуй обновить ещё раз."
            case .decoding:
                return "Ответ сервера изменился и не был прочитан приложением. Нужно обновить приложение."
            }
        }

        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                return "Нет доступа к интернету."
            case .timedOut, .networkConnectionLost:
                return "Сервер не ответил вовремя. Попробуй обновить ещё раз."
            default:
                return "Не удалось загрузить данные с сервера."
            }
        }

        return "Не удалось загрузить данные с сервера."
    }
}

private enum DashboardLoadError: LocalizedError {
    case httpStatus(Int)
    case invalidResponse
    case decoding(String)
}

private struct DashboardFetchResult {
    let payload: DashboardPayload
    let data: Data
}
