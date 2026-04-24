import SwiftUI

struct LiquidGlassBackground: View {
    var body: some View {
        LinearGradient(
            colors: [
                Color(red: 0.02, green: 0.05, blue: 0.11),
                Color(red: 0.03, green: 0.09, blue: 0.17),
                Color(red: 0.01, green: 0.04, blue: 0.09),
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
        .overlay(alignment: .topLeading) {
            Circle()
                .fill(Color.cyan.opacity(0.22))
                .frame(width: 240, height: 240)
                .blur(radius: 70)
                .offset(x: -50, y: -80)
        }
        .overlay(alignment: .topTrailing) {
            Circle()
                .fill(Color.blue.opacity(0.18))
                .frame(width: 280, height: 280)
                .blur(radius: 80)
                .offset(x: 80, y: -120)
        }
        .overlay(alignment: .bottomLeading) {
            Circle()
                .fill(Color.white.opacity(0.05))
                .frame(width: 260, height: 260)
                .blur(radius: 90)
                .offset(x: -80, y: 120)
        }
        .ignoresSafeArea()
    }
}

struct GlassCard<Content: View>: View {
    let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        content
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 24, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 24, style: .continuous)
                    .strokeBorder(
                        LinearGradient(
                            colors: [Color.white.opacity(0.24), Color.cyan.opacity(0.10)],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        ),
                        lineWidth: 1
                    )
            )
            .shadow(color: .black.opacity(0.22), radius: 24, x: 0, y: 16)
    }
}

struct ScreenContainer<Content: View>: View {
    let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        ZStack {
            LiquidGlassBackground()
            ScrollView {
                VStack(spacing: 16) {
                    content
                }
                .padding(.horizontal, 16)
                .padding(.vertical, 18)
            }
            .scrollIndicators(.hidden)
        }
    }
}

struct EmptyGlassState: View {
    let title: String
    let subtitle: String
    let systemImage: String

    var body: some View {
        GlassCard {
            VStack(spacing: 12) {
                Image(systemName: systemImage)
                    .font(.system(size: 24, weight: .semibold))
                    .foregroundStyle(.cyan)
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
            }
            .frame(maxWidth: .infinity)
        }
    }
}

struct MetricGlassTile: View {
    let title: String
    let value: String
    var tone: Color = .white
    var help: String? = nil
    @State private var showsHelp = false

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline, spacing: 6) {
                Text(title)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if help != nil {
                    Image(systemName: "info.circle")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.cyan.opacity(0.85))
                }
            }
            Text(value)
                .font(.headline.weight(.semibold))
                .foregroundStyle(tone)
                .minimumScaleFactor(0.72)
                .lineLimit(2)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(14)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        .contentShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
        .onTapGesture {
            if help != nil {
                showsHelp = true
            }
        }
        .alert(title, isPresented: $showsHelp) {
            Button("Понятно", role: .cancel) {}
        } message: {
            Text(help ?? "")
        }
    }
}

struct SignalPill: View {
    let text: String
    let raw: String?

    var body: some View {
        Text(text)
            .font(.caption.weight(.bold))
            .padding(.horizontal, 10)
            .padding(.vertical, 5)
            .background(badgeColor(for: raw).opacity(0.18), in: Capsule())
            .foregroundStyle(badgeColor(for: raw))
    }
}

struct InfoRow: View {
    let title: String
    let value: String
    var accent: Color? = nil

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer(minLength: 12)
            Text(value)
                .font(.subheadline)
                .foregroundStyle(accent ?? .primary)
                .multilineTextAlignment(.trailing)
        }
    }
}

struct SectionHeader: View {
    let title: String
    let subtitle: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.title3.weight(.semibold))
            if let subtitle, !subtitle.isEmpty {
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

struct DateFilterBar: View {
    let dates: [String]
    let selectedDate: String?
    let onSelect: (String) -> Void

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("Дата аналитики")
                    .font(.headline)
                Picker("Дата", selection: Binding(
                    get: { selectedDate ?? dates.first ?? "" },
                    set: onSelect
                )) {
                    ForEach(dates, id: \.self) { item in
                        Text(displayDate(item)).tag(item)
                    }
                }
                .pickerStyle(.menu)
            }
        }
    }
}

struct MiniPnlChart: View {
    let series: [DailyPoint]
    let selectedDate: String?

    var body: some View {
        GeometryReader { proxy in
            let padded = series.isEmpty ? placeholderSeries : series
            let maxValue = padded.map(\.cumulativePnlRub).max() ?? 0
            let minValue = padded.map(\.cumulativePnlRub).min() ?? 0
            let range = max(1, maxValue - minValue)
            let width = proxy.size.width
            let height = proxy.size.height

            ZStack {
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color.white.opacity(0.02))

                grid(in: CGRect(origin: .zero, size: proxy.size))

                Path { path in
                    for (index, point) in padded.enumerated() {
                        let x = CGFloat(index) / CGFloat(max(1, padded.count - 1)) * width
                        let y = height - CGFloat((point.cumulativePnlRub - minValue) / range) * (height - 28) - 14
                        if index == 0 {
                            path.move(to: CGPoint(x: x, y: y))
                        } else {
                            path.addLine(to: CGPoint(x: x, y: y))
                        }
                    }
                }
                .stroke(
                    LinearGradient(colors: [.cyan, .blue], startPoint: .leading, endPoint: .trailing),
                    style: StrokeStyle(lineWidth: 3, lineCap: .round, lineJoin: .round)
                )

                ForEach(Array(padded.enumerated()), id: \.offset) { index, point in
                    let x = CGFloat(index) / CGFloat(max(1, padded.count - 1)) * width
                    let y = height - CGFloat((point.cumulativePnlRub - minValue) / range) * (height - 28) - 14

                    Circle()
                        .fill(point.date == selectedDate ? Color.white : Color.cyan)
                        .frame(width: point.date == selectedDate ? 10 : 7, height: point.date == selectedDate ? 10 : 7)
                        .overlay(
                            Circle()
                                .stroke(Color.cyan.opacity(0.4), lineWidth: point.date == selectedDate ? 4 : 0)
                        )
                        .position(x: x, y: y)
                }
            }
        }
        .frame(height: 180)
    }

    @ViewBuilder
    private func grid(in rect: CGRect) -> some View {
        Path { path in
            for step in 1...3 {
                let y = rect.height * CGFloat(step) / 4
                path.move(to: CGPoint(x: 0, y: y))
                path.addLine(to: CGPoint(x: rect.width, y: y))
            }
        }
        .stroke(Color.white.opacity(0.08), style: StrokeStyle(lineWidth: 1, dash: [5, 5]))
    }

    private var placeholderSeries: [DailyPoint] {
        [
            DailyPoint(date: "0", closedCount: 0, wins: 0, losses: 0, pnlRub: 0, pnlPct: 0, cumulativePnlRub: 0, cumulativePnlPct: 0),
            DailyPoint(date: "1", closedCount: 0, wins: 0, losses: 0, pnlRub: 0, pnlPct: 0, cumulativePnlRub: 0, cumulativePnlPct: 0)
        ]
    }
}

struct SegmentedGlassPicker: View {
    let title: String
    @Binding var selection: Int
    let items: [String]

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title)
                .font(.headline)
            Picker(title, selection: $selection) {
                ForEach(Array(items.enumerated()), id: \.offset) { index, item in
                    Text(item).tag(index)
                }
            }
            .pickerStyle(.segmented)
        }
    }
}
