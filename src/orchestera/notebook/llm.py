"""
The model a notebook analysis runs on: the one Marimo's AI chat panel uses.

The control plane writes the workspace's keys (BYOK) and chat model into
the notebook's Marimo user config (sparklith-next dataplane/notebook_ai.py),
and the user may change either in Marimo's settings. Going through Marimo's
own config and provider code keeps the cell and the panel on the same model,
key and provider options -- custom and OpenAI-compatible providers included.

Those are Marimo internals, so the runtime image pins Marimo and its smoke
test imports exactly these names.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class NotebookModelError(RuntimeError):
    pass


@dataclass
class NotebookModel:
    model: Any  # pydantic_ai.models.Model
    settings: Any  # pydantic_ai.settings.ModelSettings | None
    name: str


def _detail(exc: Exception) -> str:
    # Marimo reports config problems as an HTTPException with a `detail`.
    return str(getattr(exc, "detail", None) or exc)


def notebook_model(model: str | None = None) -> NotebookModel:
    """
    `model` is a Marimo model id (`anthropic/claude-...`); default is the
    chat panel's chat model.
    """
    try:
        from marimo._config.manager import get_default_config_manager
        from marimo._server.ai.config import (
            AnyProviderConfig,
            get_chat_model,
            get_max_tokens,
        )
        from marimo._server.ai.providers import get_completion_provider
    except ImportError as exc:
        raise NotebookModelError(
            f"Job analysis needs Marimo's AI support ({exc})."
        ) from exc

    config = get_default_config_manager(current_path=None).get_config(
        hide_secrets=False
    )
    ai_config = config.get("ai", {})
    model_id = model or get_chat_model(ai_config)
    try:
        provider = get_completion_provider(
            AnyProviderConfig.for_model(model_id, ai_config), model_id
        )
        pydantic_model = provider.create_model()
        settings = provider._build_model_settings(
            pydantic_model, get_max_tokens(config)
        )
    except Exception as exc:
        raise NotebookModelError(f"{model_id}: {_detail(exc)}") from exc
    return NotebookModel(model=pydantic_model, settings=settings, name=model_id)
