from enum import Enum


class ValidFileTypes(Enum):
    TOML = "TOML"
    YAML = "YAML"

    @classmethod
    def possible_values(cls):
        return [variant.value for variant in cls.__members__.values()]
