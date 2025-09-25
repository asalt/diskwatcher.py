"""User-configurable settings helpers for DiskWatcher."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional


CONFIG_ENV_VAR = "DISKWATCHER_CONFIG_DIR"
CONFIG_FILENAME = "config.json"

__all__ = [
    "CONFIG_ENV_VAR",
    "ConfigError",
    "LOG_LEVEL_VALUES",
    "config_dir",
    "config_path",
    "list_config",
    "get_value",
    "set_value",
    "unset_value",
]


class ConfigError(RuntimeError):
    """Raised when configuration cannot be read or updated."""


def config_dir() -> Path:
    override = os.environ.get(CONFIG_ENV_VAR)
    if override:
        return Path(override).expanduser()
    return Path.home() / ".diskwatcher"


def config_path() -> Path:
    return config_dir() / CONFIG_FILENAME


def _load_user_config() -> Dict[str, Any]:
    path = config_path()
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Config file {path} is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ConfigError(f"Config file {path} must contain a JSON object")

    return payload


def _write_user_config(data: Dict[str, Any]) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def _parse_bool(value: str) -> bool:
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}
    lower = value.strip().lower()
    if lower in truthy:
        return True
    if lower in falsy:
        return False
    raise ConfigError("Expected a boolean (true/false)")


_LOG_LEVEL_ALIASES = {"warn": "warning"}


def _parse_log_level(value: str) -> str:
    normalized = value.strip().lower()
    normalized = _LOG_LEVEL_ALIASES.get(normalized, normalized)
    if normalized not in LOG_LEVEL_VALUES:
        raise ConfigError(
            f"Unsupported log level '{value}'. Choose from {', '.join(LOG_LEVEL_VALUES)}"
        )
    return normalized


@dataclass(frozen=True)
class Option:
    key: str
    parser: Callable[[str], Any]
    default: Any
    description: str
    value_type: str
    choices: Optional[Iterable[Any]] = None

    def validate_user_value(self, value: Any) -> Any:
        if value is None:
            return self.default
        if self.value_type == "boolean" and not isinstance(value, bool):
            raise ConfigError(f"Config key '{self.key}' expects a boolean value")
        if self.value_type == "string" and not isinstance(value, str):
            raise ConfigError(f"Config key '{self.key}' expects a string value")
        if self.choices and value not in self.choices:
            raise ConfigError(
                f"Config key '{self.key}' must be one of {', '.join(map(str, self.choices))}"
            )
        return value


LOG_LEVEL_VALUES = ("debug", "info", "warning", "error", "critical")


OPTIONS: Dict[str, Option] = {
    "log.level": Option(
        key="log.level",
        parser=_parse_log_level,
        default="info",
        description="Default log level when --log-level is not provided.",
        value_type="string",
        choices=LOG_LEVEL_VALUES,
    ),
    "run.auto_scan": Option(
        key="run.auto_scan",
        parser=_parse_bool,
        default=True,
        description="Control whether the run command performs the initial archival scan.",
        value_type="boolean",
    ),
}


def _get_option(key: str) -> Option:
    try:
        return OPTIONS[key]
    except KeyError as exc:
        raise ConfigError(f"Unknown config key '{key}'") from exc


def _validated_user_values() -> Dict[str, Any]:
    raw = _load_user_config()
    validated: Dict[str, Any] = {}
    for key, value in raw.items():
        if key not in OPTIONS:
            continue
        option = OPTIONS[key]
        validated[key] = option.validate_user_value(value)
    return validated


def list_config() -> Dict[str, Dict[str, Any]]:
    """Return metadata keyed by option name."""

    user_values = _validated_user_values()
    result: Dict[str, Dict[str, Any]] = {}

    for key, option in OPTIONS.items():
        if key in user_values:
            value = user_values[key]
            source = "user"
        else:
            value = option.default
            source = "default"

        result[key] = {
            "value": value,
            "default": option.default,
            "description": option.description,
            "type": option.value_type,
            "choices": tuple(option.choices) if option.choices else None,
            "source": source,
        }

    return result


def get_value(key: str) -> Any:
    option = _get_option(key)
    user_values = _validated_user_values()
    return user_values.get(key, option.default)


def set_value(key: str, raw_value: str) -> Any:
    option = _get_option(key)
    parsed = option.parser(raw_value)
    payload = _load_user_config()
    payload[key] = parsed
    _write_user_config(payload)
    return parsed


def unset_value(key: str) -> None:
    _get_option(key)
    payload = _load_user_config()
    if key in payload:
        del payload[key]
        _write_user_config(payload)
