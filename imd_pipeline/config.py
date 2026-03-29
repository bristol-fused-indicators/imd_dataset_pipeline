from dataclasses import dataclass


@dataclass
class Config:
    window_months: int
    snapshot_date: str
    lad_names: list[str]
