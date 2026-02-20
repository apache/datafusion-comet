# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
