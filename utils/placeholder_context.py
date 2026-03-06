class ResolvedPlaceholderContext:
    def __init__(self):
        self.values = {}

    def add(self, placeholder: str, resolved_value: str):
        self.values[placeholder] = resolved_value  # Drop file_name key nesting

    def get(self, placeholder: str) -> str:
        return self.values.get(placeholder)

    def all(self):
        return self.values
