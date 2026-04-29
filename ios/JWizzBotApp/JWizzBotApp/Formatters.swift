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

func displaySignal(_ raw: String?) -> String {
    switch (raw ?? "").uppercased() {
    case "LONG": return "ЛОНГ"
    case "SHORT": return "ШОРТ"
    case "HOLD": return "ОЖИДАНИЕ"
    case "FLAT": return "ВНЕ ПОЗИЦИИ"
    case "ACTIVE": return "АКТИВНА"
    case "CLOSED": return "ЗАКРЫТА"
    case "HISTORY": return "ИСТОРИЯ"
    case "BLOCK": return "БЛОК"
    default: return raw ?? "-"
    }
}

func displayBias(_ raw: String?) -> String {
    let rawValue = (raw ?? "").uppercased()
    if rawValue.isEmpty || rawValue == "NEUTRAL" { return "НЕЙТРАЛЬНО" }
    let parts = rawValue.split(separator: "/").map(String.init)
    let mapped = parts.map { part in
        switch part {
        case "LONG": return "ЛОНГ"
        case "SHORT": return "ШОРТ"
        case "BLOCK": return "БЛОК"
        case "HIGH": return "СИЛЬНЫЙ"
        case "MEDIUM": return "СРЕДНИЙ"
        case "LOW": return "СЛАБЫЙ"
        default: return part
        }
    }
    return mapped.joined(separator: " / ")
}

func displayEvent(_ raw: String?) -> String {
    switch (raw ?? "").uppercased() {
    case "OPEN": return "ОТКРЫТИЕ"
    case "CLOSE": return "ЗАКРЫТИЕ"
    default: return raw ?? "-"
    }
}

func displayRuntimeState(_ raw: String?) -> String {
    switch (raw ?? "").lowercased() {
    case "starting": return "СТАРТ"
    case "running": return "РАБОТАЕТ"
    case "api_error": return "СБОЙ API"
    case "internal_error": return "ВНУТРЕННЯЯ ОШИБКА"
    case "stopped_after_errors": return "ОСТАНОВЛЕН"
    case "startup_api_retry": return "ПОВТОР API"
    case "startup_internal_retry": return "ПОВТОР СТАРТА"
    default: return raw ?? "-"
    }
}

func humanizeAllocatorText(_ raw: String?) -> String {
    guard let raw, !raw.isEmpty else { return "-" }
    return raw
        .replacingOccurrences(of: "health ", with: "форма связки ")
        .replacingOccurrences(of: "edge high", with: "качество входа высокое")
        .replacingOccurrences(of: "edge confirmed", with: "качество входа подтверждённое")
        .replacingOccurrences(of: "edge moderate", with: "качество входа умеренное")
        .replacingOccurrences(of: "edge fragile", with: "качество входа слабое")
        .replacingOccurrences(of: "recovery mode", with: "режим восстановления")
        .replacingOccurrences(of: "trend_expansion", with: "расширение тренда")
        .replacingOccurrences(of: "trend_pullback", with: "откат в тренде")
        .replacingOccurrences(of: "compression", with: "сжатие")
        .replacingOccurrences(of: "chop", with: "пила")
        .replacingOccurrences(of: "mixed", with: "смешанный режим")
        .replacingOccurrences(of: "impulse", with: "импульс")
}

func badgeColor(for raw: String?) -> Color {
    switch (raw ?? "").uppercased() {
    case "LONG", "ACTIVE":
        return Color.green
    case "SHORT", "FAILED", "BLOCK", "CLOSED":
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

func displayDate(_ raw: String?) -> String {
    guard let raw, !raw.isEmpty else { return "-" }
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "ru_RU")
    formatter.dateFormat = "yyyy-MM-dd"
    guard let date = formatter.date(from: raw) else { return raw }
    formatter.dateFormat = "dd.MM.yyyy"
    return formatter.string(from: date)
}

func reviewAttributedText(_ markdown: String) -> AttributedString {
    if let parsed = try? AttributedString(markdown: markdown) {
        return parsed
    }
    return AttributedString(markdown)
}

func displayAIReviewStatus(_ raw: String?) -> String {
    switch (raw ?? "").lowercased() {
    case "ready": return "ГОТОВ"
    case "missing": return "НЕ НАЙДЕН"
    case "empty": return "ПУСТО"
    case "error": return "ОШИБКА"
    default: return raw ?? "-"
    }
}

func formatTradePnl(_ raw: String?) -> String {
    guard let raw, !raw.isEmpty else { return "-" }
    if let value = Double(raw) {
        return formatRub(value)
    }
    return raw
}

func safeDouble(_ raw: String?) -> Double? {
    guard let raw, !raw.isEmpty else { return nil }
    return Double(raw)
}

func formatInt(_ value: Int?) -> String {
    guard let value else { return "-" }
    return "\(value)"
}

func statusTone(for value: Double?) -> Color {
    guard let value else { return .white }
    if value > 0 { return .green }
    if value < 0 { return .red }
    return .white
}

func statusTone(forString value: String?) -> Color {
    guard let value, let number = Double(value) else { return .white }
    return statusTone(for: number)
}
