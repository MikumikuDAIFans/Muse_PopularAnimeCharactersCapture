from services.rules import RuleSet


def test_non_character_rule_filters_original():
    include, needs_review, note = RuleSet().character_decision("original")
    assert include is False
    assert needs_review is False
    assert "non-character" in note


def test_alias_override_wins():
    rules = RuleSet(alias_overrides={"miku": "hatsune_miku"})
    assert rules.canonical_character("miku", {"miku": "other"}) == "hatsune_miku"
