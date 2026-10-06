from lib.html_parser import parse_body, ParsedBody


def test_extracts_prose_text():
    html = '<p>How do I center a div?</p>'
    result = parse_body(html)
    assert 'center a div' in result.text
    assert result.code_snippets == []
    assert result.image_urls == []


def test_extracts_pre_code_block():
    html = '<p>Try this:</p><pre><code>div { margin: auto; }</code></pre>'
    result = parse_body(html)
    assert 'div { margin: auto; }' in result.code_snippets[0]
    assert 'div { margin: auto; }' not in result.text


def test_extracts_multiple_code_blocks():
    html = '<pre><code>foo = 1</code></pre><p>Also:</p><pre><code>bar = 2</code></pre>'
    result = parse_body(html)
    assert len(result.code_snippets) == 2
    assert any('foo = 1' in s for s in result.code_snippets)
    assert any('bar = 2' in s for s in result.code_snippets)


def test_extracts_image_urls():
    html = '<p>See image:</p><img src="https://i.stack.imgur.com/abc12.png" alt="example">'
    result = parse_body(html)
    assert result.image_urls == ['https://i.stack.imgur.com/abc12.png']
    assert 'i.stack.imgur.com' not in result.text


def test_extracts_multiple_images():
    html = '<img src="https://i.stack.imgur.com/img1.png"><img src="https://i.stack.imgur.com/img2.jpg">'
    result = parse_body(html)
    assert len(result.image_urls) == 2


def test_empty_body_returns_empty():
    result = parse_body('')
    assert result.text == ''
    assert result.code_snippets == []
    assert result.image_urls == []
    assert result.parse_warning is False


def test_skips_img_with_no_src():
    html = '<img alt="no src here"><img src="https://i.stack.imgur.com/x.png">'
    result = parse_body(html)
    assert result.image_urls == ['https://i.stack.imgur.com/x.png']


def test_prose_text_clean_after_code_removal():
    html = '<p>Use <code>margin: auto</code> to center.</p>'
    result = parse_body(html)
    # Inline code text should still appear in prose (it's meaningful context)
    assert 'center' in result.text


def test_returns_parse_warning_on_bad_input():
    result = parse_body(None)
    assert result.parse_warning is True
    assert result.text == ''
