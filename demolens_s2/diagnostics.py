from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from .errors import DemoLensError


def classify_exception(
    stage: str, exc: BaseException, context: Optional[Dict[str, Any]] = None
) -> Tuple[str, str, Dict[str, Any]]:
    if isinstance(exc, DemoLensError):
        details = dict(exc.details)
        if context:
            details.update(context)
        reason = exc.failure_reason
        if context and context.get("patch_version") is not None and "patch_version" not in reason:
            reason = "%s (patch_version=%s)" % (reason, context.get("patch_version"))
        return exc.category, reason, details

    message = str(exc) or exc.__class__.__name__
    lowered = message.lower()
    details = {
        "exception_type": exc.__class__.__name__,
        "exception_message": message,
    }
    if context:
        details.update(context)
    patch_suffix = ""
    if context and context.get("patch_version") is not None:
        patch_suffix = " (patch_version=%s)" % context.get("patch_version")
    if "entitynotfound" in lowered or "entity not found" in lowered:
        return (
            "entity_not_found",
            "%s: entity not found while parsing %s%s" % (stage, stage, patch_suffix),
            details,
        )
    if "unsupported patch" in lowered or ("unsupported" in lowered and "patch" in lowered):
        return (
            "unsupported_patch",
            "%s: unsupported patch while parsing %s%s" % (stage, stage, patch_suffix),
            details,
        )
    if "utf8" in lowered or "unicode" in lowered:
        return (
            "parse_failed",
            "%s: utf-8 decode failure while parsing %s%s" % (stage, stage, patch_suffix),
            details,
        )
    if "os error 2" in lowered or "file not found" in lowered:
        return (
            "parse_failed",
            "%s: demo file not found%s" % (stage, patch_suffix),
            details,
        )
    return ("parse_failed", "%s: parse failed%s" % (stage, patch_suffix), details)
