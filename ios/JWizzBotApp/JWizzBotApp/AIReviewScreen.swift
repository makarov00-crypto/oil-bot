import SwiftUI

struct AIReviewScreen: View {
    @ObservedObject var store: DashboardStore
    @State private var followupQuestion = ""

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

                            HStack(spacing: 10) {
                                Button {
                                    Task { await store.refreshAIReview(date: store.selectedDate) }
                                } label: {
                                    if store.isRefreshingAIReview {
                                        ProgressView()
                                            .controlSize(.small)
                                    } else {
                                        Label("Обновить AI-разбор", systemImage: "arrow.triangle.2.circlepath")
                                    }
                                }
                                .buttonStyle(.borderedProminent)
                                .tint(.cyan)
                                .disabled(store.isRefreshingAIReview)

                                if let message = store.aiReviewRefreshMessage, !message.isEmpty {
                                    Text(message)
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
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
                        AIReviewMarkdownView(markdown: payload.aiReview.content)

                        GlassCard {
                            VStack(alignment: .leading, spacing: 12) {
                                Text("Дополнительный вопрос к AI-разбору")
                                    .font(.headline)
                                TextEditor(text: $followupQuestion)
                                    .frame(minHeight: 110)
                                    .padding(10)
                                    .background(.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

                                HStack(spacing: 10) {
                                    Button {
                                        let question = followupQuestion
                                        Task {
                                            await store.requestAIReviewFollowup(question: question, date: store.selectedDate)
                                            if let message = store.aiReviewFollowupMessage, message.contains("готов") {
                                                followupQuestion = ""
                                            }
                                        }
                                    } label: {
                                        if store.isRequestingAIFollowup {
                                            ProgressView()
                                                .controlSize(.small)
                                        } else {
                                            Label("Задать дополнительный вопрос", systemImage: "text.bubble")
                                        }
                                    }
                                    .buttonStyle(.borderedProminent)
                                    .tint(.cyan)
                                    .disabled(store.isRequestingAIFollowup)

                                    if let message = store.aiReviewFollowupMessage, !message.isEmpty {
                                        Text(message)
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }
                                }
                            }
                        }

                        if let followups = payload.aiReview.followups, !followups.isEmpty {
                            ForEach(Array(followups.reversed())) { item in
                                VStack(alignment: .leading, spacing: 10) {
                                    Text(item.question)
                                        .font(.headline)
                                        .padding(.horizontal, 4)
                                    if let created = item.createdAtMoscow ?? item.model {
                                        Text([created, item.model].compactMap { $0 }.joined(separator: " • "))
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                            .padding(.horizontal, 4)
                                    }
                                    AIReviewMarkdownView(markdown: item.answer)
                                }
                            }
                        }
                    } else {
                        EmptyGlassState(
                            title: "AI-разбор пока не найден",
                            subtitle: "Разбор за эту дату пока не опубликован на сервере.",
                            systemImage: "brain.head.profile"
                        )
                    }
                }
                .refreshable { await store.load(date: store.selectedDate) }
            } else if store.isLoading {
                ZStack {
                    LiquidGlassBackground()
                    ProgressView("Загружаю AI-разбор…")
                }
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
                HStack(spacing: 12) {
                    Button {
                        Task { await store.refreshAIReview(date: store.selectedDate) }
                    } label: {
                        Image(systemName: "brain")
                    }
                    .disabled(store.isRefreshingAIReview)

                    Button {
                        Task { await store.load(date: store.selectedDate) }
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }
}

private struct AIReviewMarkdownView: View {
    let markdown: String

    var body: some View {
        let blocks = parseBlocks(markdown)
        return GlassCard {
            VStack(alignment: .leading, spacing: 18) {
                ForEach(Array(blocks.enumerated()), id: \.offset) { _, block in
                    switch block {
                    case .h1(let text):
                        Text(text)
                            .font(.title2.weight(.bold))
                            .foregroundStyle(.white)
                    case .h2(let text):
                        Text(text)
                            .font(.title3.weight(.semibold))
                            .foregroundStyle(.white)
                    case .bulletList(let items):
                        VStack(alignment: .leading, spacing: 10) {
                            ForEach(items, id: \.self) { item in
                                HStack(alignment: .top, spacing: 10) {
                                    Circle()
                                        .fill(Color.cyan)
                                        .frame(width: 7, height: 7)
                                        .padding(.top, 6)
                                    Text(cleanInlineMarkdown(item))
                                        .font(.body)
                                        .foregroundStyle(.primary)
                                        .fixedSize(horizontal: false, vertical: true)
                                }
                            }
                        }
                    case .numbered(let title, let items):
                        VStack(alignment: .leading, spacing: 10) {
                            Text(title)
                                .font(.headline)
                                .foregroundStyle(.white)
                            ForEach(Array(items.enumerated()), id: \.offset) { index, item in
                                HStack(alignment: .top, spacing: 10) {
                                    Text("\(index + 1).")
                                        .font(.subheadline.weight(.semibold))
                                        .foregroundStyle(.cyan)
                                    Text(cleanInlineMarkdown(item))
                                        .font(.body)
                                        .foregroundStyle(.primary)
                                        .fixedSize(horizontal: false, vertical: true)
                                }
                            }
                        }
                    case .paragraph(let text):
                        Text(cleanInlineMarkdown(text))
                            .font(.body)
                            .foregroundStyle(.primary)
                            .fixedSize(horizontal: false, vertical: true)
                    case .meta(let text):
                        Text(cleanInlineMarkdown(text))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .textSelection(.enabled)
        }
    }

    private enum Block {
        case h1(String)
        case h2(String)
        case bulletList([String])
        case numbered(String, [String])
        case paragraph(String)
        case meta(String)
    }

    private func parseBlocks(_ markdown: String) -> [Block] {
        let lines = markdown
            .replacingOccurrences(of: "\r\n", with: "\n")
            .components(separatedBy: .newlines)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { line in
                let lowercased = line.lowercased()
                return lowercased != "```" && lowercased != "```markdown" && lowercased != "```md"
            }

        var blocks: [Block] = []
        var currentBullets: [String] = []
        var currentParagraph: [String] = []

        func flushBullets() {
            guard !currentBullets.isEmpty else { return }
            blocks.append(.bulletList(currentBullets))
            currentBullets.removeAll()
        }

        func flushParagraph() {
            guard !currentParagraph.isEmpty else { return }
            let paragraph = currentParagraph.joined(separator: " ")
            if paragraph.hasPrefix("Модель:") || paragraph.hasPrefix("Сформировано:") || paragraph.hasPrefix("- Модель:") || paragraph.hasPrefix("- Сформировано:") {
                blocks.append(.meta(paragraph))
            } else {
                blocks.append(.paragraph(paragraph))
            }
            currentParagraph.removeAll()
        }

        for rawLine in lines {
            if rawLine.isEmpty {
                flushBullets()
                flushParagraph()
                continue
            }

            if rawLine.hasPrefix("# ") {
                flushBullets()
                flushParagraph()
                blocks.append(.h1(String(rawLine.dropFirst(2))))
                continue
            }

            if rawLine.hasPrefix("## ") {
                flushBullets()
                flushParagraph()
                blocks.append(.h2(String(rawLine.dropFirst(3))))
                continue
            }

            if rawLine.hasPrefix("### ") {
                flushBullets()
                flushParagraph()
                blocks.append(.h2(String(rawLine.dropFirst(4))))
                continue
            }

            if rawLine.hasPrefix("- Модель:") || rawLine.hasPrefix("- Сформировано:") {
                flushBullets()
                flushParagraph()
                blocks.append(.meta(String(rawLine.dropFirst(2))))
                continue
            }

            if rawLine.hasPrefix("- ") {
                flushParagraph()
                currentBullets.append(String(rawLine.dropFirst(2)))
                continue
            }

            if let dotRange = rawLine.range(of: ". "), rawLine.prefix(upTo: dotRange.lowerBound).allSatisfy(\.isNumber) {
                flushBullets()
                flushParagraph()
                blocks.append(.h2(String(rawLine[dotRange.upperBound...])))
                continue
            }

            currentParagraph.append(rawLine)
        }

        flushBullets()
        flushParagraph()
        return compressNumberedSections(blocks)
    }

    private func compressNumberedSections(_ blocks: [Block]) -> [Block] {
        var result: [Block] = []
        var pendingTitle: String?
        var pendingItems: [String] = []

        func flushPending() {
            guard let pendingTitle else { return }
            result.append(.numbered(pendingTitle, pendingItems))
            selfPendingTitleReset()
        }

        func selfPendingTitleReset() {
            pendingTitle = nil
            pendingItems = []
        }

        for block in blocks {
            switch block {
            case .h2(let text):
                if text.contains("Короткий итог дня") ||
                    text.contains("Лучшие инструменты дня") ||
                    text.contains("Худшие инструменты дня") ||
                    text.contains("Главные ошибки") ||
                    text.contains("Что менять завтра") ||
                    text.contains("Что НЕ менять завтра") ||
                    text.contains("Уровень риска на завтра") {
                    flushPending()
                    pendingTitle = text
                    pendingItems = []
                } else {
                    flushPending()
                    result.append(block)
                }
            case .bulletList(let items):
                if pendingTitle != nil {
                    pendingItems.append(contentsOf: items)
                } else {
                    result.append(block)
                }
            case .paragraph(let text):
                if pendingTitle != nil {
                    if pendingItems.isEmpty {
                        pendingItems.append(text)
                    } else {
                        flushPending()
                        result.append(.paragraph(text))
                    }
                } else {
                    result.append(block)
                }
            default:
                flushPending()
                result.append(block)
            }
        }

        flushPending()
        return result
    }

    private func cleanInlineMarkdown(_ text: String) -> String {
        text
            .replacingOccurrences(of: "`", with: "")
            .replacingOccurrences(of: "**", with: "")
            .replacingOccurrences(of: "  ", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
