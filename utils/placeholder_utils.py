import logging
import re
from calendar import isleap
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

FAILED_PLACEHOLDER_MSG = "Failed to resolve placeholder: %s"


def resolve_placeholders(value, file_name):
    """
    Replace placeholders of the form <<PLACEHOLDER>> in a string with resolved values.
    If resolution fails, the original placeholder text is left unchanged.
    """

    if not isinstance(value, str):
        return value

    def replacer(match):
        placeholder = match.group(1)
        try:
            resolved = _resolve_placeholder_value(placeholder)
        except Exception:
            logger.exception(
                "[ERROR] Could not resolve placeholder %s: in %s:",
                placeholder,
                file_name,
            )
            return match.group(0)  # leave unchanged
        return resolved

    return re.sub(r"<<(.*?)>>", replacer, value)


def _resolve_placeholder_value(placeholder: str) -> str:

    if placeholder in ["IGNORE_RESPONSE_ID", "IGNORE_DATE", "RANDOM_GUID"]:
        return placeholder

    parts = placeholder.split("_")
    if len(parts) != 3:
        logger.exception(FAILED_PLACEHOLDER_MSG, placeholder)
        return f"<<{placeholder}>>"

    placeholder_type, unit, shift = parts

    function_type = {
        "TIME": _resolve_time,
        "DATE": _resolve_date,
        "RDATE": _resolve_date,
        "NBSDATE": _resolve_date,
    }

    handler = function_type.get(placeholder_type)
    if handler is None:
        logger.exception(FAILED_PLACEHOLDER_MSG, placeholder)
        return f"<<{placeholder}>>"

    try:
        return handler(placeholder_type, unit, shift)
    except Exception:
        logger.exception(FAILED_PLACEHOLDER_MSG, placeholder)
        return f"<<{placeholder}>>"


def _resolve_time(_type: str, unit: str, shift: str) -> str:
    now = datetime.now(ZoneInfo("Europe/London"))

    handlers = {
        "HOUR": lambda n, s: n + timedelta(hours=int(s)),
        "MINUTE": lambda n, s: n + timedelta(minutes=int(s)),
        "SECOND": lambda n, s: n + timedelta(seconds=int(s)),
    }

    fn = handlers.get(unit)
    if fn is None:
        return f"<<TIME_{unit}_{shift}>>"

    new_time = fn(now, shift)
    return new_time.strftime("%H:%M:%S")


def _resolve_date(date_type: str, unit: str, shift: str) -> str:
    now = datetime.now(ZoneInfo("Europe/London"))

    handlers = {
        "AGE": lambda n, s: _resolve_age_placeholder(n, s, date_type),
        "DAY": lambda n, s: n + timedelta(days=int(s)),
        "WEEK": lambda n, s: n + timedelta(weeks=int(s)),
        "MONTH": lambda n, s: n + relativedelta(months=int(s)),
        "YEAR": lambda n, s: n + relativedelta(years=int(s)),
    }

    fn = handlers.get(unit)
    if fn is None:
        return f"<<{date_type}_{unit}_{shift}>>"

    result = fn(now, shift)

    if isinstance(result, str):
        return result  # AGE returns final formatted date

    return _format_date(result, date_type)


def _resolve_age_placeholder(today: datetime, age_str: str, format_type: str) -> str:
    offset_days = 0

    if age_str.endswith("-TOMORROW"):
        age = int(age_str.replace("-TOMORROW", ""))
        offset_days = 1
    elif age_str.endswith("-YESTERDAY"):
        age = int(age_str.replace("-YESTERDAY", ""))
        offset_days = -1
    else:
        age = int(age_str)

    target_year = today.year - age

    try:
        result_date = today.replace(year=target_year)
    except ValueError:
        if today.month == 2 and today.day == 29 and not isleap(target_year):
            result_date = datetime(target_year, 2, 28, tzinfo=today.tzinfo)
        else:
            raise

    if offset_days:
        result_date = result_date + timedelta(days=offset_days)

    return _format_date(result_date, format_type)


def _format_date(date: datetime, format_type: str) -> str:
    formats = {
        "RDATE": "%-d %B %Y",
        "NBSDATE": "%Y-%m-%d",
        "DATE": "%Y%m%d",
    }

    fmt = formats.get(format_type, "%Y%m%d")
    return date.strftime(fmt)
