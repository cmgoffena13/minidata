from datetime import UTC, datetime
from enum import Enum

import structlog
from slack_sdk.webhook import WebhookClient

from src.notify.base import BaseNotifier
from src.settings import config
from src.utils import retry

logger = structlog.getLogger(__name__)


class AlertLevel(Enum):
    INFO = "ℹ️"  # noqa: RUF001
    WARNING = "⚠️"
    ERROR = "❌"
    CRITICAL = "🚨"
    SUCCESS = "✅"


class SlackNotifier(BaseNotifier):
    def __init__(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        details: dict[str, object] | None = None,
    ) -> None:
        self.level = level
        self.title = title
        self.message = message
        self.details = details
        self.slack_message = self._create_message()

    def _create_message(self) -> str:
        timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        formatted_message = [
            f"{self.level.value} *{self.level.name}*",
            f"*{self.title}*",
            f"*Timestamp:* {timestamp}",
            f"*Message:* {self.message}",
        ]
        if self.details:
            detail_lines = [f"• *{key}:* {value}" for key, value in self.details.items()]
            if detail_lines:
                formatted_message.append("\n*Details:*")
                formatted_message.extend(detail_lines)
        return "\n".join(formatted_message)

    @retry()
    def _send_slack(self, webhook_url: str) -> None:
        response = WebhookClient(webhook_url).send(text=self.slack_message)
        if response.status_code == 200:
            logger.info("Sent Slack notification")
        else:
            raise RuntimeError(
                f"Slack webhook returned status {response.status_code}: {response.body}"
            )

    def notify(self) -> None:
        webhook_url = config.SLACK_WEBHOOK_URL
        if not webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured, skipping Slack notification")
            return
        try:
            self._send_slack(webhook_url)
        except Exception:
            logger.exception("Failed to send Slack notification after retries")
