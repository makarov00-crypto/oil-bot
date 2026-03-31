import Foundation
import SwiftUI

func formatRub(_ value: Double?) -> String {
    guard let value else { return "-" }
    return String(format: "%.2f RUB", value)
}

func formatPrice(_ value: Double?) -> String {
    guard let value else { return "-" }
    return String(format: "%.4f", value)
}

func formatPct(_ value: Double?) -> String {
    guard let value else { return "-" }
    let sign = value > 0 ? "+" : ""
    return String(format: "\(sign)%.2f%%", value)
}

func displayMode(_ raw: String?) -> String {
    switch (raw ?? "").uppercased() {
    case "DRY_RUN": return "ТЕСТ"
    case "LIVE": return "БОЙ"
    default: return raw ?? "-"
    }
}

func displaySession(_ raw: String?) -> String {
    switch (raw ?? "").uppercased() {
    case "MORNING": return "УТРО"
    case "DAY": return "ДЕНЬ"
    case "EVENING": return "ВЕЧЕР"
    case "CLOSED": return "ЗАКРЫТО"
    case "WEEKEND": return "ВЫХОДНОЙ"
    default: return raw ?? "-"
    }
}

func badgeColor(for raw: String?) -> Color {
    switch (raw ?? "").uppercased() {
    case "LONG", "ACTIVE":
        return Color.green
    case "SHORT", "FAILED", "BLOCK":
        return Color.red
    default:
        return Color.orange
    }
}

func formatLoadTime(_ value: Date?) -> String {
    guard let value else { return "-" }
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "ru_RU")
    formatter.dateFormat = "dd.MM HH:mm"
    return formatter.string(from: value)
}
