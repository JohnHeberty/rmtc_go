from dataclasses import dataclass

@dataclass(frozen=True)
class Archive:
    index:      bool = False
    encoding:   str = "latin-1"
    sep:        str = ","
    decimal:    str = "."
