from services.dataset import build_caption


def test_caption_order_subject_character_copyright_artist_general_other():
    groups = {
        "subject": ["1girl"],
        "character": ["hatsune_miku"],
        "copyright": ["vocaloid"],
        "artist": ["ixy"],
        "general": ["long_hair", "smile"],
        "other": ["highres"],
    }
    assert build_caption(groups) == "1girl, hatsune_miku, vocaloid, ixy, long_hair, smile, highres"


def test_artist_can_be_disabled():
    groups = {
        "subject": ["1girl"],
        "character": ["hatsune_miku"],
        "copyright": ["vocaloid"],
        "artist": ["ixy"],
        "general": ["long_hair"],
        "other": [],
    }
    assert build_caption(groups, include_artist=False) == "1girl, hatsune_miku, vocaloid, long_hair"
