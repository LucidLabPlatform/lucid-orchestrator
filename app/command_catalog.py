"""Command catalog for LUCID agents and components.

Provides payload templates and metadata for all known MQTT commands.
Agent commands are static (agents don't publish capabilities).
Component commands are derived from ``component_metadata.capabilities``
stored in Postgres, enriched with payload templates for known actions.
"""

from __future__ import annotations


def _label(action: str) -> str:
    """Generate a human-readable label from an action name."""
    return action.replace("/", " ").replace("-", " ").replace("_", " ").title()


# ── Agent-level commands ─────────────────────────────────────────────

AGENT_COMMANDS: list[dict] = [
    # lifecycle
    {"action": "ping", "category": "lifecycle", "label": "Ping", "has_body": False, "template": None},
    {"action": "restart", "category": "lifecycle", "label": "Restart", "has_body": False, "template": None},
    {"action": "refresh", "category": "lifecycle", "label": "Refresh", "has_body": False, "template": None},
    # config
    {"action": "cfg/set", "category": "config", "label": "Set Config", "has_body": True,
     "template": {"set": {"heartbeat_s": 30}}},
    {"action": "cfg/logging/set", "category": "config", "label": "Set Log Level", "has_body": True,
     "template": {"set": {"log_level": "INFO"}}},
    {"action": "cfg/telemetry/set", "category": "config", "label": "Set Telemetry", "has_body": True,
     "template": {"set": {"cpu_percent": {"enabled": True, "interval_s": 60, "change_threshold_percent": 5}}}},
    # component management
    {"action": "components/install", "category": "components", "label": "Install Component", "has_body": True,
     "template": {"component_id": "", "source": {"type": "github_release", "owner": "", "repo": "", "version": "", "sha256": ""}}},
    {"action": "components/uninstall", "category": "components", "label": "Uninstall Component", "has_body": True,
     "template": {"component_id": ""}},
    {"action": "components/enable", "category": "components", "label": "Enable Component", "has_body": True,
     "template": {"component_id": ""}},
    {"action": "components/disable", "category": "components", "label": "Disable Component", "has_body": True,
     "template": {"component_id": ""}},
    {"action": "components/upgrade", "category": "components", "label": "Upgrade Component", "has_body": True,
     "template": {"component_id": "", "source": {"type": "github_release", "owner": "", "repo": "", "version": "", "sha256": ""}}},
    # core upgrade
    {"action": "core/upgrade", "category": "upgrade", "label": "Upgrade Core", "has_body": True,
     "template": {"source": {"type": "github_release", "version": "", "sha256": ""}}},
]


# ── Component payload templates for known actions ────────────────────
# Maps action name -> template dict (None = no body needed).

COMPONENT_TEMPLATES: dict[str, dict | None] = {
    # universal
    "ping": None,
    "reset": None,
    "clear": None,
    # config (always available via component base class)
    "cfg/set": {"set": {}},
    "cfg/logging/set": {"set": {"log_level": "INFO"}},
    "cfg/telemetry/set": {"set": {}},
    # LED strip
    "set-color": {"color": {"r": 255, "g": 0, "b": 0}},
    "set-range-percent": {"color": {"r": 255, "g": 0, "b": 0}, "start_percent": 0, "end_percent": 100},
    "set-range-exact": {"color": {"r": 255, "g": 0, "b": 0}, "start_idx": 0, "end_idx": 10},
    "effect/glow": {"color": {"r": 255, "g": 255, "b": 255}, "speed": 1.0},
    "effect/wave": {"color": {"r": 0, "g": 0, "b": 255}, "speed": 1.0},
    "effect/color-wipe": {"color": {"r": 0, "g": 0, "b": 255}, "speed": 1.0},
    "effect/color-fade": {"colors": [{"r": 255, "g": 0, "b": 0}, {"r": 0, "g": 0, "b": 255}], "speed": 1.0},
    "effect/sparkle": {"color": {"r": 255, "g": 255, "b": 255}, "speed": 1.0},
    "effect/rainbow": {"speed": 1.0},
    "effect/rainbow-cycle": {"speed": 1.0},
    "effect/theater-chase": {"color": {"r": 255, "g": 255, "b": 255}, "speed": 1.0},
    "effect/running": {"color": {"r": 255, "g": 0, "b": 0}, "speed": 1.0},
    # ROS bridge
    "roslaunch_start": {"package": "", "launch_file": ""},
    "roslaunch_stop": None,
    "rosbag_start": {"output_dir": "", "topics": []},
    "rosbag_stop": None,
    # TouchDesigner
    "launch": {"project_file": ""},
    "terminate": None,
    "ndi/input/set": {"ndi_inputs": {}},
    "ndi/output/set": {"ndi_outputs": {}},
    # AI specialist
    "task": {"prompt": ""},
    "process_image": {"image_url": "", "prompt": ""},
}

# Base-class commands that every component supports (not in capabilities list)
_BASE_COMMANDS = ["cfg/set", "cfg/logging/set", "cfg/telemetry/set"]


def get_agent_commands() -> list[dict]:
    """Return the full agent command catalog."""
    return list(AGENT_COMMANDS)


def get_component_commands(capabilities: list[str] | None) -> list[dict]:
    """Build command list for a component from its capabilities.

    Always includes base-class config commands. Unknown actions get an
    empty ``{}`` template so the user can fill in the payload manually.
    """
    actions: list[str] = list(capabilities or [])

    # Ensure base-class commands are present
    for base_action in _BASE_COMMANDS:
        if base_action not in actions:
            actions.append(base_action)

    commands: list[dict] = []
    for action in actions:
        template = COMPONENT_TEMPLATES.get(action, {})
        has_body = template is not None and template != {}
        # For unknown actions, default to has_body=True with empty template
        if action not in COMPONENT_TEMPLATES:
            has_body = True
            template = {}

        if action.startswith("effect/"):
            category = "effects"
        elif action.startswith("cfg/"):
            category = "config"
        elif action in ("ping", "reset", "clear", "start", "stop"):
            category = "lifecycle"
        else:
            category = "custom"

        commands.append({
            "action": action,
            "category": category,
            "label": _label(action),
            "has_body": has_body,
            "template": template,
        })

    return commands
