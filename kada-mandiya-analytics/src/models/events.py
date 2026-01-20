from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.utils.time import ensure_utc, validate_event_timestamp

SourceType = Literal["web", "mobile", "gateway", "service"]


class BaseEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_timestamp: datetime
    session_id: str
    user_id: str | None = None
    source: SourceType
    properties: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _infer_session_id(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        session_id = data.get("session_id")
        if (session_id is None or str(session_id).strip() == "") and data.get(
            "correlation_id"
        ):
            data = dict(data)
            data["session_id"] = data["correlation_id"]
        return data

    @model_validator(mode="after")
    def _validate_timestamp(self) -> "BaseEvent":
        validate_event_timestamp(self.event_timestamp)
        self.event_timestamp = ensure_utc(self.event_timestamp)
        return self


class PageViewEvent(BaseEvent):
    event_type: Literal["page_view"]

    page_url: str
    referrer_url: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None
    time_on_prev_page_seconds: int | None = None


class ClickEvent(BaseEvent):
    event_type: Literal["click"]

    page_url: str
    element_id: str | None = None
    x: int | None = None
    y: int | None = None
    viewport_w: int | None = None
    viewport_h: int | None = None
    user_agent: str | None = None
    ip_address: str | None = None


class ScrollEvent(BaseEvent):
    event_type: Literal["scroll"]

    page_url: str
    scroll_depth_pct: float | None = None


class FormInteractionEvent(BaseEvent):
    event_type: Literal["form_interaction"]

    page_url: str
    form_id: str | None = None
    field_id: str | None = None
    action: str | None = None
    error_message: str | None = None
    time_spent_ms: int | None = None


class SearchEvent(BaseEvent):
    event_type: Literal["search"]

    page_url: str
    query: str | None = None
    results_count: int | None = None
    filters: dict[str, Any] | str | None = None


class PerformanceEvent(BaseEvent):
    event_type: Literal["performance"]

    service: str = "web"
    page_url: str | None = None
    metric_name: str
    metric_value: float
    metric_unit: str | None = None
    correlation_id: str | None = None
    entity_id: str | None = None


class CartActionEvent(BaseEvent):
    event_type: Literal["cart_action"]

    service: str = "web"
    action: Literal["add", "remove"]
    cart_id: str | None = None
    product_id: str
    quantity: int = 1
    price: float | None = None
    currency: str | None = None
    correlation_id: str | None = None
    entity_id: str | None = None


class CheckoutEvent(BaseEvent):
    event_type: Literal["checkout"]

    service: str = "web"
    stage: str
    order_id: str | None = None
    cart_id: str | None = None
    total_amount: float | None = None
    currency: str | None = None
    correlation_id: str | None = None
    entity_id: str | None = None


class PurchaseViewEvent(BaseEvent):
    event_type: Literal["purchase_view"]

    service: str = "web"
    order_id: str | None = None
    product_id: str | None = None
    revenue: float | None = None
    currency: str | None = None
    correlation_id: str | None = None
    entity_id: str | None = None


class FrontendErrorEvent(BaseEvent):
    event_type: Literal["frontend_error"]

    service: str = "web"
    page_url: str | None = None
    error_type: str | None = None
    message: str
    stack: str | None = None
    severity: str | None = None
    correlation_id: str | None = None
    entity_id: str | None = None


class BusinessEvent(BaseEvent):
    service: str
    correlation_id: str | None = None
    entity_id: str | None = None
    payload: dict[str, Any] | str | None = None

    @model_validator(mode="before")
    @classmethod
    def _ensure_payload(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if data.get("payload") is None:
            data = dict(data)
            props = data.get("properties")
            data["payload"] = props if props is not None else dict(data)
        return data


class ApiRequestLogEvent(BaseEvent):
    event_type: Literal["api_request_log"]

    service: str
    endpoint: str
    method: str
    status_code: int
    response_time_ms: int
    correlation_id: str | None = None
    request_size_bytes: int | None = None
    response_size_bytes: int | None = None
    ip_address: str | None = None
    user_agent: str | None = None


class DbQueryPerfEvent(BaseEvent):
    event_type: Literal["db_query_perf"]

    service: str
    database_name: str | None = None
    query_type: str | None = None
    table_name: str | None = None
    execution_time_ms: int
    rows_affected: int | None = None
    query_hash: str | None = None
