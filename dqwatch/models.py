from dataclasses import dataclass, field


@dataclass
class Column:
    name: str
    type: str  # integer | number | text | boolean | date | timestamp


@dataclass
class Source:
    id: int | None
    name: str
    kind: str  # folder | url | api
    location: str
    schedule_minutes: int
    write_mode: str  # replace | append | upsert
    key_column: str | None
    columns: list[Column]
    sample: list[dict] = field(default_factory=list)


@dataclass
class CheckResult:
    rule_id: str
    status: str  # pass | fail | error
    severity: str  # warn | error
    failing_count: int = 0
    sample: list[dict] = field(default_factory=list)
    detail: str = ""


@dataclass
class StageReport:
    new_columns: list[str]
    missing_columns: list[str]
    cast_failed_rows: int
    null_key_rows: list[dict]
    duplicate_keys: int
