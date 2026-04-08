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
    @Published private(set) var isRefreshingAIReview = false
    @Published private(set) var aiReviewRefreshMessage: String?
    @Published private(set) var isRequestingAIFollowup = false
    @Published private(set) var aiReviewFollowupMessage: String?
    @Published private(set) var isRecoveringTrades = false
    @Published private(set) var tradeRecoveryMessage: String?

    private let dashboardURL = URL(string: "https://jwizzbot.ru/api/dashboard")!
    private let aiReviewRefreshURL = URL(string: "https://jwizzbot.ru/api/ai-review/refresh")!
    private let aiReviewFollowupURL = URL(string: "https://jwizzbot.ru/api/ai-review/followup")!
    private let tradeRecoveryURL = URL(string: "https://jwizzbot.ru/api/trades/recover")!
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

    func refreshAIReview(date: String? = nil) async {
        if isRefreshingAIReview { return }
        isRefreshingAIReview = true
        defer { isRefreshingAIReview = false }

        let targetDate = date ?? selectedDate
        do {
            let requestURL = makeAIReviewRefreshURL(date: targetDate)
            var request = URLRequest(url: requestURL)
            request.httpMethod = "POST"
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw DashboardLoadError.invalidResponse
            }
            guard (200..<300).contains(http.statusCode) else {
                let message = (try? JSONDecoder().decode(AIReviewRefreshResponse.self, from: data).message)
                    ?? "Не удалось запустить AI-разбор."
                throw DashboardLoadError.decoding(message)
            }
            let payload = try JSONDecoder().decode(AIReviewRefreshResponse.self, from: data)
            aiReviewRefreshMessage = payload.message
        } catch {
            aiReviewRefreshMessage = describeAIRefresh(error)
        }
    }

    func recoverTradeOperations(date: String? = nil) async {
        if isRecoveringTrades { return }
        isRecoveringTrades = true
        defer { isRecoveringTrades = false }

        let targetDate = date ?? selectedDate
        do {
            let requestURL = makeTradeRecoveryURL(date: targetDate)
            var request = URLRequest(url: requestURL)
            request.httpMethod = "POST"
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw DashboardLoadError.invalidResponse
            }
            guard (200..<300).contains(http.statusCode) else {
                let message = (try? JSONDecoder().decode(TradeRecoveryResponse.self, from: data).message)
                    ?? "Не удалось восстановить операции."
                throw DashboardLoadError.decoding(message)
            }
            let payload = try JSONDecoder().decode(TradeRecoveryResponse.self, from: data)
            tradeRecoveryMessage = payload.message
            await load(date: targetDate)
        } catch {
            tradeRecoveryMessage = describeTradeRecovery(error)
        }
    }

    func requestAIReviewFollowup(question: String, date: String? = nil) async {
        if isRequestingAIFollowup { return }
        let cleanQuestion = question.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleanQuestion.isEmpty else {
            aiReviewFollowupMessage = "Сначала введи вопрос к AI-разбору."
            return
        }
        isRequestingAIFollowup = true
        defer { isRequestingAIFollowup = false }

        let targetDate = date ?? selectedDate
        do {
            let requestURL = makeAIReviewFollowupURL(date: targetDate)
            var request = URLRequest(url: requestURL)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONEncoder().encode(["question": cleanQuestion])
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw DashboardLoadError.invalidResponse
            }
            guard (200..<300).contains(http.statusCode) else {
                let message = (try? JSONDecoder().decode(AIReviewFollowupResponse.self, from: data).message)
                    ?? "Не удалось получить дополнительный AI-разбор."
                throw DashboardLoadError.decoding(message)
            }
            let payload = try JSONDecoder().decode(AIReviewFollowupResponse.self, from: data)
            aiReviewFollowupMessage = payload.message
            await load(date: targetDate)
        } catch {
            aiReviewFollowupMessage = describeAIFollowup(error)
        }
    }

    private func makeDashboardURL(date: String?) -> URL {
        guard let date, !date.isEmpty else { return dashboardURL }
        var components = URLComponents(url: dashboardURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "date", value: date)]
        return components?.url ?? dashboardURL
    }

    private func makeAIReviewRefreshURL(date: String?) -> URL {
        guard let date, !date.isEmpty else { return aiReviewRefreshURL }
        var components = URLComponents(url: aiReviewRefreshURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "date", value: date)]
        return components?.url ?? aiReviewRefreshURL
    }

    private func makeTradeRecoveryURL(date: String?) -> URL {
        guard let date, !date.isEmpty else { return tradeRecoveryURL }
        var components = URLComponents(url: tradeRecoveryURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "date", value: date)]
        return components?.url ?? tradeRecoveryURL
    }

    private func makeAIReviewFollowupURL(date: String?) -> URL {
        guard let date, !date.isEmpty else { return aiReviewFollowupURL }
        var components = URLComponents(url: aiReviewFollowupURL, resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "date", value: date)]
        return components?.url ?? aiReviewFollowupURL
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

    private func describeAIRefresh(_ error: Error) -> String {
        if let error = error as? DashboardLoadError {
            switch error {
            case .httpStatus(let code):
                return "Сервер вернул ошибку \(code) при запуске AI-разбора."
            case .invalidResponse:
                return "Сервер вернул неполный ответ при запуске AI-разбора."
            case .decoding(let message):
                return message
            }
        }

        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                return "Нет доступа к интернету."
            case .timedOut, .networkConnectionLost:
                return "Сервер не ответил вовремя при запуске AI-разбора."
            default:
                return "Не удалось запустить AI-разбор."
            }
        }

        return "Не удалось запустить AI-разбор."
    }

    private func describeTradeRecovery(_ error: Error) -> String {
        if let error = error as? DashboardLoadError {
            switch error {
            case .httpStatus(let code):
                return "Сервер вернул ошибку \(code) при восстановлении операций."
            case .invalidResponse:
                return "Сервер вернул неполный ответ при восстановлении операций."
            case .decoding(let message):
                return message
            }
        }

        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                return "Нет доступа к интернету."
            case .timedOut, .networkConnectionLost:
                return "Сервер не ответил вовремя при восстановлении операций."
            default:
                return "Не удалось восстановить операции."
            }
        }

        return "Не удалось восстановить операции."
    }

    private func describeAIFollowup(_ error: Error) -> String {
        if let error = error as? DashboardLoadError {
            switch error {
            case .httpStatus(let code):
                return "Сервер вернул ошибку \(code) при дополнительном AI-разборе."
            case .invalidResponse:
                return "Сервер вернул неполный ответ при дополнительном AI-разборе."
            case .decoding(let message):
                return message
            }
        }

        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                return "Нет доступа к интернету."
            case .timedOut, .networkConnectionLost:
                return "Сервер не ответил вовремя при дополнительном AI-разборе."
            default:
                return "Не удалось получить дополнительный AI-разбор."
            }
        }

        return "Не удалось получить дополнительный AI-разбор."
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

private struct TradeRecoveryResponse: Decodable {
    let message: String
}

private struct AIReviewRefreshResponse: Decodable {
    let started: Bool?
    let status: String?
    let message: String?
    let date: String?
}

private struct AIReviewFollowupResponse: Decodable {
    let ok: Bool?
    let date: String?
    let message: String?
}
