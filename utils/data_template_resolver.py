import copy
import json
from typing import Any, Dict, List, Optional
from functools import lru_cache

DEFAULT_INHERITANCE = {
    "RSV": "VACCINE_BASE",
    "COVID": "VACCINE_BASE",
    "FLU": "VACCINE_BASE",
}

DEFAULT_TEMPLATE = "data/dynamoDB/dynamo_data_template.json"


class TemplateEngine:
    KEY_PRIORITY = ("id", "name", "key")

    @classmethod
    @lru_cache(maxsize=1)
    def create(cls):
        # Instantiate engine with default template and inheritance (cached).
        with open(DEFAULT_TEMPLATE) as template_file:
            templates = json.load(template_file)
        return cls(templates, DEFAULT_INHERITANCE)

    def __init__(
        self,
        templates: List[Dict[str, Any]],
        inheritance: Optional[Dict[str, str]] = None,
    ):
        self.raw = {t["ATTRIBUTE_TYPE"]: t for t in templates}
        self.inheritance = inheritance or {}
        self.templates = self._resolve_templates()

    def apply(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply templates to a list of data records."""
        return [self._merge_record(r) for r in data]

    # Merge individual record
    def _merge_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        attr = record.get("ATTRIBUTE_TYPE")
        if attr not in self.templates:
            known = ", ".join(sorted(self.templates))
            raise ValueError(f"No template for ATTRIBUTE_TYPE '{attr}'. Known: {known}")
        return self._merge(self.templates[attr], record)

    # Deep merge
    def _merge(self, template: Any, record: Any) -> Any:
        if isinstance(template, dict) and isinstance(record, dict):
            merged = {k: copy.deepcopy(v) for k, v in template.items()}
            for k, v in record.items():
                merged[k] = self._merge(merged[k], v) if k in merged else v
            return merged

        if isinstance(template, list) and isinstance(record, list):
            # Schema-style template (e.g., cohort membership)
            if len(template) == 1 and isinstance(template[0], dict):
                schema = template[0]
                return [self._merge(schema, r) for r in record]

            # Otherwise, try key-based merge
            key = self._detect_key(template, record)
            if key:
                return self._merge_list(template, record, key)

            return record

        # primitive or mismatched type
        return record

    # Merge list by key
    def _merge_list(
        self, template: List[Any], record: List[Any], key: str
    ) -> List[Any]:
        template_map = {
            item[key]: item
            for item in template
            if isinstance(item, dict) and key in item
        }
        result = []
        for item in record:
            if isinstance(item, dict) and key in item and item[key] in template_map:
                result.append(self._merge(template_map[item[key]], item))
            else:
                result.append(item)
        return result

    # Detect list merge key
    def _detect_key(self, template: List[Any], record: List[Any]) -> Optional[str]:
        sample = None
        if template and isinstance(template[0], dict):
            sample = template[0]
        elif record and isinstance(record[0], dict):
            sample = record[0]
        if not sample:
            return None
        keys = sample.keys()
        for k in self.KEY_PRIORITY:
            if k in keys:
                return k
        for k in keys:
            if k.endswith(("_ID", "_CODE", "_LABEL")):
                return k
        return None

    # Resolve template inheritance
    def _resolve_templates(self) -> Dict[str, Dict[str, Any]]:
        resolved = {}

        def resolve(attr: str, stack: Optional[List[str]] = None) -> Dict[str, Any]:
            if attr in resolved:
                return resolved[attr]
            if attr not in self.raw:
                raise ValueError(f"Template not found: {attr}")
            stack = stack or []
            if attr in stack:
                cycle = " -> ".join(stack + [attr])
                raise ValueError(f"Inheritance cycle: {cycle}")
            base = self.inheritance.get(attr)
            if base:
                parent = resolve(base, stack + [attr])
                merged = self._merge(parent, self.raw[attr])
            else:
                merged = self.raw[attr]
            resolved[attr] = merged
            return merged

        for attr in self.raw:
            resolve(attr)

        # exclude base types from final templates
        base_types = set(self.inheritance.values())
        return {k: v for k, v in resolved.items() if k not in base_types}
