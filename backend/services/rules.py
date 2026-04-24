"""角色榜单与 caption 清洗规则。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Set

try:
    import yaml
except Exception:  # pragma: no cover - yaml normally comes from uvicorn[standard]
    yaml = None


DEFAULT_NON_CHARACTER_TAGS = {
    "original",
    "character_request",
    "commentary_request",
    "artist_request",
    "cosplay",
}

DEFAULT_AMBIGUOUS_PATTERNS = {
    "no_humans",
    "multiple_persona",
}

DEFAULT_SUBJECT_TAGS = {
    "1girl",
    "1boy",
    "2girls",
    "2boys",
    "3girls",
    "3boys",
    "solo",
    "multiple_girls",
    "multiple_boys",
    "multiple_views",
}


@dataclass
class RuleSet:
    non_character_tags: Set[str] = field(default_factory=lambda: set(DEFAULT_NON_CHARACTER_TAGS))
    ambiguous_tags: Set[str] = field(default_factory=set)
    subject_tags: Set[str] = field(default_factory=lambda: set(DEFAULT_SUBJECT_TAGS))
    tag_blacklist: Set[str] = field(default_factory=set)
    alias_overrides: Dict[str, str] = field(default_factory=dict)

    def canonical_character(self, tag: str, alias_map: Dict[str, str] | None = None) -> str:
        if tag in self.alias_overrides:
            return self.alias_overrides[tag]
        if alias_map and tag in alias_map:
            return alias_map[tag]
        return tag

    def character_decision(self, tag: str) -> tuple[bool, bool, str]:
        """返回 (include, needs_review, note)。"""
        if tag in self.non_character_tags:
            return False, False, "filtered: non-character rule"
        if tag in self.ambiguous_tags:
            return False, True, "filtered: ambiguous rule"
        if tag.endswith("_request") or tag.startswith("tagme"):
            return False, False, "filtered: request/tagme"
        return True, False, ""

    def clean_caption_tags(self, tags: Iterable[str]) -> List[str]:
        cleaned: List[str] = []
        for tag in tags:
            if not tag or tag in self.tag_blacklist:
                continue
            if tag.endswith("_request"):
                continue
            cleaned.append(tag)
        return list(dict.fromkeys(cleaned))


def _read_structured(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    if yaml is not None:
        data = yaml.safe_load(text)
        return data or {}
    return {}


def load_rules(root: Path | str = "rules") -> RuleSet:
    root_path = Path(root)
    rules = RuleSet()
    mapping = {
        "character_filter.yml": ("non_character_tags", set),
        "ambiguous_character_tags.yml": ("ambiguous_tags", set),
        "subject_tags.yml": ("subject_tags", set),
        "tag_cleaning.yml": ("tag_blacklist", set),
        "alias_overrides.yml": ("alias_overrides", dict),
    }
    for filename, (attr, caster) in mapping.items():
        payload = _read_structured(root_path / filename)
        if not payload:
            continue
        key = attr
        value = payload.get(key, payload.get("items", payload))
        if caster is set:
            setattr(rules, attr, set(value or []))
        else:
            setattr(rules, attr, dict(value or {}))
    return rules
