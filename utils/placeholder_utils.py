import logging
import re
from calendar import isleap
from datetime import UTC, datetime, timedelta

from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def resolve_placeholders(value, context=None, file_name=None):
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
            logger.exception("[ERROR] Could not resolve placeholder %s:", placeholder)
            return match.group(0)  # leave placeholder unchanged
        else:
            if context:
                context.add(placeholder, resolved, file_name)
            return resolved

    return re.sub(r"<<(.*?)>>", replacer, value)


def _resolve_placeholder_value(placeholder: str) -> str:
    placeholder_parts_length = 3
    valid_placeholder_types = ["DATE", "RDATE", "IGNORE"]
    result = f"<<{placeholder}>>"  # Default fallback

    if placeholder in ["IGNORE_RESPONSE_ID", "IGNORE_DATE"]:
        return placeholder

    parts = placeholder.split("_")
    if (
        len(parts) != placeholder_parts_length
        or parts[0] not in valid_placeholder_types
    ):
        return result

    today = datetime.now(UTC)
    date_type, arg = parts[1], parts[2]

    try:
        if date_type == "AGE":
            result = _resolve_age_placeholder(today, arg, parts[0])
        elif date_type == "DAY":
            result = _format_date(today + timedelta(days=int(arg)), parts[0])
        elif date_type == "WEEK":
            result = _format_date(today + timedelta(weeks=int(arg)), parts[0])
        elif date_type == "MONTH":
            result = _format_date(today + relativedelta(months=int(arg)), parts[0])
        elif date_type == "YEAR":
            result = _format_date(today + relativedelta(years=int(arg)), parts[0])
    except Exception:
        logger.exception("Failed to resolve placeholder: %s", placeholder)
        raise
    return result


def _resolve_age_placeholder(today: datetime, age_str: str, format_type: str) -> str:
    """
    Resolve placeholders like:
      - DATE_AGE_75
      - DATE_AGE_75-TOMORROW
      - DATE_AGE_75-YESTERDAY
    """

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
    february = 2
    leap_year_day = 29
    try:
        result_date = today.replace(year=target_year)
    except ValueError:
        if (
            today.month == february
            and today.day == leap_year_day
            and not isleap(target_year)
        ):
            result_date = datetime(target_year, 2, 28, tzinfo=UTC)
        else:
            raise

    if offset_days:
        result_date = result_date + timedelta(days=offset_days)

    return _format_date(result_date, format_type)


def _format_date(date: datetime, format_type: str) -> str:
    return (
        date.strftime("%Y%m%d") if format_type == "DATE" else date.strftime("%-d %B %Y")
    )
