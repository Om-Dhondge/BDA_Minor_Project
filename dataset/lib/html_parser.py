from dataclasses import dataclass, field
from bs4 import BeautifulSoup


@dataclass
class ParsedBody:
    text: str = ""
    code_snippets: list[str] = field(default_factory=list)
    image_urls: list[str] = field(default_factory=list)
    parse_warning: bool = False


def parse_body(html: str | None) -> ParsedBody:
    """Extract prose text, code snippets, and image URLs from an HTML body string.

    - <pre> blocks → code_snippets (removed from prose)
    - <img src="..."> → image_urls (removed from prose)
    - remaining HTML → plain text
    """
    if not html:
        return ParsedBody(parse_warning=html is None)

    try:
        soup = BeautifulSoup(html, 'lxml')
    except Exception:
        return ParsedBody(text=str(html), parse_warning=True)

    # 1. Extract image URLs and remove img tags in one pass
    image_urls = []
    for img in soup.find_all('img'):
        src = img.get('src')
        if src:
            image_urls.append(src)
        img.decompose()

    # 2. Extract code from <pre> blocks, then remove them
    code_snippets = []
    for pre in soup.find_all('pre'):
        snippet = pre.get_text(separator='\n').strip()
        if snippet:
            code_snippets.append(snippet)
        pre.decompose()

    # 4. Get remaining prose text
    text = soup.get_text(separator=' ', strip=True)
    text = ' '.join(text.split())  # normalize whitespace

    return ParsedBody(
        text=text,
        code_snippets=code_snippets,
        image_urls=image_urls,
        parse_warning=False,
    )
