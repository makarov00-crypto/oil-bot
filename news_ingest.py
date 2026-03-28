from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html import unescape
from typing import Iterable

import requests

from news_bias import NewsBias, NewsMessage, detect_news_bias


CHANNEL_URLS: dict[str, str] = {
    "markettwits": "https://t.me/s/markettwits",
    "moex_derivatives": "https://t.me/s/moex_derivatives",
}

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
WRAP_SPLIT = '<div class="tgme_widget_message_wrap js-widget_message_wrap">'
TEXT_RE = re.compile(r'<div class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', re.S)
DATE_RE = re.compile(r'<time datetime="([^"]+)" class="time">')
POST_RE = re.compile(r'data-post="[^/]+/(\d+)"')
BEFORE_RE = re.compile(r'class="tme_messages_more js-messages_more" data-before="(\d+)"')
TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class ChannelPost:
    channel: str
    message_id: int
    created_at: datetime
    text: str
    url: str


@dataclass(frozen=True)
class ChannelPage:
    channel: str
    posts: list[ChannelPost]
    next_before: int | None


def strip_html(html: str) -> str:
    text = html.replace("<br/>", "\n").replace("<br>", "\n").replace("</p>", "\n")
    text = TAG_RE.sub("", text)
    return "\n".join(line.strip() for line in unescape(text).splitlines() if line.strip())


def fetch_channel_page(channel: str, before: int | None = None, timeout: int = 20) -> ChannelPage:
    if channel not in CHANNEL_URLS:
        raise KeyError(f"Неизвестный канал: {channel}")
    url = CHANNEL_URLS[channel]
    if before:
        url = f"{url}?before={before}"
    response = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    html = response.text

    posts: list[ChannelPost] = []
    for chunk in html.split(WRAP_SPLIT)[1:]:
        post_match = POST_RE.search(chunk)
        date_match = DATE_RE.search(chunk)
        text_match = TEXT_RE.search(chunk)
        if not post_match or not date_match or not text_match:
            continue

        created_at = datetime.fromisoformat(date_match.group(1)).astimezone(UTC)
        message_id = int(post_match.group(1))
        text = strip_html(text_match.group(1))
        if not text:
            continue

        posts.append(
            ChannelPost(
                channel=channel,
                message_id=message_id,
                created_at=created_at,
                text=text,
                url=f"https://t.me/{channel}/{message_id}",
            )
        )

    before_match = BEFORE_RE.search(html)
    next_before = int(before_match.group(1)) if before_match else None
    return ChannelPage(channel=channel, posts=posts, next_before=next_before)


def fetch_posts_for_day(channel: str, target_day: date, max_pages: int = 6) -> list[ChannelPost]:
    before: int | None = None
    collected: list[ChannelPost] = []

    for _ in range(max_pages):
        page = fetch_channel_page(channel, before=before)
        if not page.posts:
            break

        for post in page.posts:
            post_day = post.created_at.astimezone().date()
            if post_day == target_day:
                collected.append(post)

        oldest_day = min(post.created_at.astimezone().date() for post in page.posts)
        if oldest_day < target_day or page.next_before is None:
            break
        before = page.next_before

    unique = {(post.channel, post.message_id): post for post in collected}
    return sorted(unique.values(), key=lambda item: item.created_at)


def build_news_messages(posts: Iterable[ChannelPost]) -> list[NewsMessage]:
    return [
        NewsMessage(
            channel=post.channel,
            text=post.text,
            created_at=post.created_at,
            message_id=post.message_id,
        )
        for post in posts
    ]


def detect_biases_for_posts(posts: Iterable[ChannelPost]) -> list[tuple[ChannelPost, list[NewsBias]]]:
    results: list[tuple[ChannelPost, list[NewsBias]]] = []
    for post in posts:
        biases = detect_news_bias(
            NewsMessage(
                channel=post.channel,
                text=post.text,
                created_at=post.created_at,
                message_id=post.message_id,
            )
        )
        if biases:
            results.append((post, biases))
    return results
