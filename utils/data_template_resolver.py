import copy
import json
from functools import lru_cache

DEFAULT_INHERITANCE = {
    "RSV": "VACCINE_BASE",
    "COVID": "VACCINE_BASE",
    "FLU": "VACCINE_BASE",
}

DEFAULT_TEMPLATE = "data/dynamoDB/dynamo_data_template.json"


class TemplateEngine:
    """Simple Dynamo template engine based on ATTRIBUTE_TYPE merging."""

    MERGE_KEY = "ATTRIBUTE_TYPE"

    def __init__(self, templates, inheritance):
        self.templates = templates
        self.inheritance = inheritance
        self._built_templates = None  # cache for built template lookup

    @classmethod
    @lru_cache(maxsize=1)
    def create(cls):
        try:
            with open(DEFAULT_TEMPLATE) as f:
                templates = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Default template file not found: {DEFAULT_TEMPLATE}"
            )
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON in default template file: {DEFAULT_TEMPLATE}"
            )
        except (OSError, IOError) as e:
            raise IOError(f"Error reading template file: {DEFAULT_TEMPLATE}, {e}")
        return cls(templates, DEFAULT_INHERITANCE)

    def apply(self, scenario_data):
        """Apply template definitions to scenario data."""

        # Build templates only once and cache them
        if self._built_templates is None:
            built = self.build()
            self._built_templates = {item[self.MERGE_KEY]: item for item in built}

        templates = self._built_templates
        resolved = []

        for item in scenario_data:
            attr_type = item.get(self.MERGE_KEY)

            if attr_type not in templates:
                raise ValueError(f"Unknown template type: {attr_type}")

            merged = copy.deepcopy(templates[attr_type])
            merged.update(item)

            resolved.append(merged)

        return resolved

    def build(self):
        """Build the fully merged template."""
        merged = self._index_by_attribute_type(self.templates)

        for child, parent in self.inheritance.items():
            if parent not in merged:
                raise ValueError(f"Parent template '{parent}' not found")

            if child not in merged:
                merged[child] = {self.MERGE_KEY: child}

            merged[child] = self._merge_objects(
                merged[parent],
                merged[child],
            )

        return list(merged.values())

    @staticmethod
    def _index_by_attribute_type(items):
        """Convert template list into a dict keyed by ATTRIBUTE_TYPE."""
        index = {}

        for item in items:
            key = item.get(TemplateEngine.MERGE_KEY)

            if not key:
                raise ValueError(f"Missing {TemplateEngine.MERGE_KEY} in template item")

            index[key] = copy.deepcopy(item)

        return index

    @staticmethod
    def _merge_objects(parent, child):
        """Merge parent template into child."""
        merged = copy.deepcopy(parent)
        merged.update(child)
        return merged
