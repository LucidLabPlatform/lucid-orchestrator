from __future__ import annotations

import json
import os
import re
from typing import Any

import yaml

from app.experiments.models import TemplateDef


def load_template(path: str) -> TemplateDef:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return load_template_from_dict(raw)


def load_template_from_dict(data: dict) -> TemplateDef:
    _normalise_parameters(data)
    return TemplateDef.model_validate(data)


def _normalise_parameters(data: dict) -> None:
    params = data.get("parameters") or {}
    normalised: dict[str, dict] = {}
    for key, val in params.items():
        if isinstance(val, dict) and "type" in val:
            normalised[key] = val
        else:
            default = val
            normalised[key] = {
                "type": _infer_type(default),
                "default": default,
                "description": "",
                "required": default is None,
            }
    data["parameters"] = normalised


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    return "string"


_PARAM_RE = re.compile(r"\$\{([^}]+)\}")
_STEP_REF_RE = re.compile(r"^\$\{steps\.([^.]+)\.result\.(.+)\}$")


def substitute_params(template: TemplateDef, params: dict[str, Any]) -> TemplateDef:
    resolved = _resolve_values(template, params)
    raw = json.loads(template.model_dump_json())
    substituted = _replace_in(raw, resolved)
    return load_template_from_dict(substituted)


def _resolve_values(template: TemplateDef, params: dict[str, Any]) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for name, schema in template.parameters.items():
        if name in params:
            resolved[name] = params[name]
        elif schema.default is not None:
            resolved[name] = schema.default
        elif schema.required:
            raise ValueError(f"Required parameter '{name}' was not provided")
    for name, val in params.items():
        if name not in resolved:
            resolved[name] = val
    return resolved


def _replace_in(obj: Any, values: dict[str, Any]) -> Any:
    if isinstance(obj, str):
        # If the entire string is a single ${param} reference, return the raw value
        # (preserving type: int, float, bool, etc.) instead of stringifying it.
        single = _PARAM_RE.fullmatch(obj)
        if single:
            key = single.group(1)
            if not key.startswith("steps.") and key in values:
                return values[key]

        def _sub(match: re.Match) -> str:
            key = match.group(1)
            if key.startswith("steps."):
                return match.group(0)
            return str(values.get(key, match.group(0)))

        return _PARAM_RE.sub(_sub, obj)
    if isinstance(obj, dict):
        return {k: _replace_in(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_in(item, values) for item in obj]
    return obj


def resolve_step_ref(ref: str, step_results: dict[str, Any]) -> Any:
    match = _STEP_REF_RE.match(ref)
    if not match:
        return ref
    step_name, field_path = match.group(1), match.group(2)
    result = step_results.get(step_name)
    if result is None:
        return ref
    for part in field_path.split("."):
        if isinstance(result, dict):
            result = result.get(part)
        else:
            return ref
        if result is None:
            return ref
    return result


def resolve_params_in_step(step_params: dict[str, Any], step_results: dict[str, Any]) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, val in step_params.items():
        if isinstance(val, str) and val.startswith("${steps."):
            resolved[key] = resolve_step_ref(val, step_results)
        else:
            resolved[key] = val
    return resolved


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def load_seed_templates() -> list[TemplateDef]:
    if not os.path.isdir(_TEMPLATES_DIR):
        return []
    templates = []
    for fname in sorted(os.listdir(_TEMPLATES_DIR)):
        if fname.endswith((".yaml", ".yml", ".json")):
            templates.append(load_template(os.path.join(_TEMPLATES_DIR, fname)))
    return templates
