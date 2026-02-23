"""Quick smoke test for extra-fields support and env-configurable defaults."""
from unittest.mock import patch

from assessment.models import ChatLLMOptions, ChatOptions, ChatPromptOptions, DatasetOptions, ParserConfig
from assessment.services import _apply_default_chat_options, _apply_default_dataset_options


# ---------------------------------------------------------------------------
# Model extra-fields pass-through
# ---------------------------------------------------------------------------

def test_parser_config_extra_fields():
    p = ParserConfig(enable_metadata=True, some_extra="val")
    d = p.model_dump(exclude_none=True)
    assert d == {"enable_metadata": True, "some_extra": "val"}


def test_dataset_options_extra_fields():
    o = DatasetOptions(parser_config=ParserConfig(enable_metadata=True), pagerank=5)
    d = o.model_dump(exclude_none=True)
    assert d["parser_config"]["enable_metadata"] is True
    assert d["pagerank"] == 5


def test_chat_llm_options_extra_fields():
    llm = ChatLLMOptions(temperature=0.7, custom_llm_field="abc")
    d = llm.model_dump(exclude_none=True)
    assert d["temperature"] == 0.7
    assert d["custom_llm_field"] == "abc"


def test_chat_prompt_options_extra_fields():
    p = ChatPromptOptions(top_n=5, highlight=True)
    d = p.model_dump(exclude_none=True)
    assert d["top_n"] == 5
    assert d["highlight"] is True


def test_chat_options_extra_fields():
    opts = ChatOptions(
        llm=ChatLLMOptions(temperature=0.5),
        prompt=ChatPromptOptions(top_n=10, custom_prompt_field="x"),
        some_top_level_extra=42,
    )
    d = opts.model_dump(exclude_none=True)
    assert d["llm"]["temperature"] == 0.5
    assert d["prompt"]["top_n"] == 10
    assert d["prompt"]["custom_prompt_field"] == "x"
    assert d["some_top_level_extra"] == 42


# ---------------------------------------------------------------------------
# Env-configurable default parser config
# ---------------------------------------------------------------------------

def test_apply_default_dataset_options_uses_env():
    """Default parser_config comes from the ASSESSMENT_DEFAULT_PARSER_CONFIG setting."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_parser_config = '{"enable_metadata": true, "chunk_token_num": 256}'
        result = _apply_default_dataset_options(None)
        assert result["parser_config"]["enable_metadata"] is True
        assert result["parser_config"]["chunk_token_num"] == 256


def test_apply_default_dataset_options_user_overrides():
    """User-supplied values take precedence over env defaults."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_parser_config = '{"enable_metadata": true, "chunk_token_num": 256}'
        user_opts = {"parser_config": {"chunk_token_num": 1024, "delimiter": "\\n"}}
        result = _apply_default_dataset_options(user_opts)
        assert result["parser_config"]["enable_metadata"] is True  # from default
        assert result["parser_config"]["chunk_token_num"] == 1024  # user override
        assert result["parser_config"]["delimiter"] == "\\n"  # user-only


def test_apply_default_dataset_options_empty_env():
    """When env default is empty JSON, no parser_config key is injected."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_parser_config = '{}'
        result = _apply_default_dataset_options(None)
        assert "parser_config" not in result or result.get("parser_config") == {}


def test_apply_default_dataset_options_invalid_json():
    """Invalid JSON in env is gracefully ignored."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_parser_config = 'NOT JSON'
        result = _apply_default_dataset_options(None)
        assert result.get("parser_config") is None or result.get("parser_config") == {}


# ---------------------------------------------------------------------------
# Env-configurable default chat config
# ---------------------------------------------------------------------------

def test_apply_default_chat_options_uses_env():
    """Default chat config comes from the ASSESSMENT_DEFAULT_CHAT_CONFIG setting."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_chat_config = '{"llm": {"temperature": 0.3}, "prompt": {"top_n": 15}}'
        result = _apply_default_chat_options(None)
        assert result["llm"]["temperature"] == 0.3
        assert result["prompt"]["top_n"] == 15


def test_apply_default_chat_options_user_overrides():
    """User-supplied values take precedence; nested dicts are shallow-merged."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_chat_config = '{"llm": {"temperature": 0.3, "max_tokens": 512}}'
        user_opts = {"llm": {"temperature": 0.9}}
        result = _apply_default_chat_options(user_opts)
        assert result["llm"]["temperature"] == 0.9  # user override
        assert result["llm"]["max_tokens"] == 512  # from default


def test_apply_default_chat_options_empty_env():
    """When env default is empty, chat_options are returned as-is."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_chat_config = '{}'
        result = _apply_default_chat_options({"llm": {"temperature": 0.5}})
        assert result == {"llm": {"temperature": 0.5}}


def test_apply_default_chat_options_none_input():
    """When no user opts and no env default, returns empty dict."""
    with patch("assessment.services.settings") as mock_settings:
        mock_settings.default_chat_config = '{}'
        result = _apply_default_chat_options(None)
        assert result == {}


if __name__ == "__main__":
    test_parser_config_extra_fields()
    test_dataset_options_extra_fields()
    test_chat_llm_options_extra_fields()
    test_chat_prompt_options_extra_fields()
    test_chat_options_extra_fields()
    test_apply_default_dataset_options_uses_env()
    test_apply_default_dataset_options_user_overrides()
    test_apply_default_dataset_options_empty_env()
    test_apply_default_dataset_options_invalid_json()
    test_apply_default_chat_options_uses_env()
    test_apply_default_chat_options_user_overrides()
    test_apply_default_chat_options_empty_env()
    test_apply_default_chat_options_none_input()
    print("OK")
