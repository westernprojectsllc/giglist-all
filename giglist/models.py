"""Show dataclass shared by both regions."""

from dataclasses import dataclass, field, asdict
from datetime import date
from typing import List, Optional


@dataclass
class Show:
    title: str
    sort_date: date
    venue: str
    url: str = ""
    sold_out: bool = False
    time: Optional[str] = None
    doors: Optional[str] = None
    supports: List[str] = field(default_factory=list)

    def to_json_dict(self) -> dict:
        d = asdict(self)
        d["sort_date"] = self.sort_date.isoformat()
        return d

    @classmethod
    def from_json_dict(cls, d: dict) -> "Show":
        return cls(
            title=d["title"],
            sort_date=date.fromisoformat(d["sort_date"]),
            venue=d["venue"],
            url=d.get("url", ""),
            sold_out=bool(d.get("sold_out", False)),
            time=d.get("time"),
            doors=d.get("doors"),
            supports=list(d.get("supports") or []),
        )
