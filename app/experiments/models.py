from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class ParameterSchema(BaseModel):
    type: str = "string"
    default: Any = None
    description: str = ""
    required: bool = False


class StepDef(BaseModel):
    name: str
    type: str = "command"
    agent_id: str | None = None
    component_id: str | None = None
    action: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_s: float | str = 30.0
    retries: int = 0
    on_failure: str = "abort"
    duration_s: float | str | None = None
    steps: list["StepDef"] | None = None
    source_topic: str | None = None
    target_topic: str | None = None
    payload_template: str | None = None
    select_clause: str = "*"
    qos: int = 0
    operation: str = "create"

    @model_validator(mode="after")
    def _validate_type_fields(self) -> "StepDef":
        if self.type == "command" and not self.action:
            raise ValueError(f"Step '{self.name}' of type 'command' must specify 'action'")
        if self.type == "delay" and self.duration_s is None:
            raise ValueError(f"Step '{self.name}' of type 'delay' must specify 'duration_s'")
        if self.type == "parallel" and not self.steps:
            raise ValueError(f"Step '{self.name}' of type 'parallel' must specify 'steps'")
        if self.type == "topic_link" and not (self.source_topic and self.target_topic):
            raise ValueError(
                f"Step '{self.name}' of type 'topic_link' must specify "
                "'source_topic' and 'target_topic'"
            )
        if self.type == "topic_link" and self.operation not in {"create", "activate", "deactivate", "delete"}:
            raise ValueError(
                f"Step '{self.name}' of type 'topic_link' must specify a valid 'operation'"
            )
        return self


class TemplateDef(BaseModel):
    id: str
    name: str
    version: str = "1.0.0"
    description: str = ""
    parameters: dict[str, ParameterSchema] = Field(default_factory=dict)
    steps: list[StepDef] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)

    def parameters_schema_dict(self) -> dict:
        return {k: v.model_dump() for k, v in self.parameters.items()}

    def to_definition_dict(self) -> dict:
        return self.model_dump()
