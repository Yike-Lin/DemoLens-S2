from __future__ import annotations

from typing import Any, Dict, Optional


class DemoLensError(Exception):
    def __init__(
        self,
        failure_stage: str,
        failure_reason: str,
        category: str = "parse_failed",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.failure_stage = failure_stage
        self.failure_reason = failure_reason
        self.category = category
        self.details = details or {}
        super(DemoLensError, self).__init__("%s: %s" % (failure_stage, failure_reason))


class UnsupportedPatchError(DemoLensError):
    def __init__(
        self,
        failure_stage: str,
        failure_reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super(UnsupportedPatchError, self).__init__(
            failure_stage, failure_reason, "unsupported_patch", details
        )


class EntityNotFoundError(DemoLensError):
    def __init__(
        self,
        failure_stage: str,
        failure_reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super(EntityNotFoundError, self).__init__(
            failure_stage, failure_reason, "entity_not_found", details
        )


class ParseFailedError(DemoLensError):
    def __init__(
        self,
        failure_stage: str,
        failure_reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super(ParseFailedError, self).__init__(
            failure_stage, failure_reason, "parse_failed", details
        )


class MissingFieldError(DemoLensError):
    def __init__(
        self,
        failure_stage: str,
        failure_reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super(MissingFieldError, self).__init__(
            failure_stage, failure_reason, "parse_failed", details
        )

