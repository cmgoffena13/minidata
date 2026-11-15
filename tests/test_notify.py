from unittest.mock import MagicMock

import pytest

from src.notify.slack import AlertLevel, SlackNotifier
from src.settings import config


def test_slack_notifier_skips_without_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "SLACK_WEBHOOK_URL", None)
    send = MagicMock()
    monkeypatch.setattr("src.notify.slack.WebhookClient", send)
    SlackNotifier(level=AlertLevel.ERROR, title="Summary", message="failed").notify()
    send.assert_not_called()


def test_slack_notifier_sends_formatted_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    client = MagicMock()
    client.send.return_value = MagicMock(status_code=200)
    monkeypatch.setattr("src.notify.slack.WebhookClient", lambda _url: client)
    monkeypatch.setattr("src.utils.time.sleep", lambda _seconds: None)
    SlackNotifier(
        level=AlertLevel.ERROR,
        title="Pipeline Processing Summary",
        message="• widgets: boom",
        details={"failures": 1},
    ).notify()
    text = client.send.call_args.kwargs["text"]
    assert "ERROR" in text
    assert "Pipeline Processing Summary" in text
    assert "• widgets: boom" in text
    assert "failures" in text
