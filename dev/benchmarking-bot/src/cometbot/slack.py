"""Slack notifications for the cometbot."""

import os

from rich.console import Console

console = Console()


def notify(message: str, level: str = "info") -> None:
    """Post a message to Slack.

    Silently no-ops if COMETBOT_SLACK_TOKEN is not set.
    Never raises exceptions — Slack failures must not break the bot.

    Args:
        message: Text to post.
        level: One of "info", "success", "warning", "error".
    """
    token = os.environ.get("COMETBOT_SLACK_TOKEN")
    channel = os.environ.get("COMETBOT_SLACK_CHANNEL")

    if not token or not channel:
        return

    level_emoji = {
        "info": ":information_source:",
        "success": ":white_check_mark:",
        "warning": ":warning:",
        "error": ":x:",
    }
    emoji = level_emoji.get(level, "")
    text = f"{emoji} {message}" if emoji else message

    try:
        from slack_sdk import WebClient

        client = WebClient(token=token)
        client.chat_postMessage(channel=channel, text=text)
    except Exception as e:
        console.print(f"[dim]Slack notification failed: {e}[/dim]")
