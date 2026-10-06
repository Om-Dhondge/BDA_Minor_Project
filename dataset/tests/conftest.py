# tests/conftest.py
import pytest
import tempfile
import os


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def sample_posts_xml(tmp_dir):
    """A minimal Posts.xml-like file with known questions and answers."""
    lines = [
        '<?xml version="1.0" encoding="utf-8"?>\n',
        '<posts>\n',
        '  <row Id="100" PostTypeId="1" AcceptedAnswerId="200" Score="5" Title="How to center a div?" Tags="|css|html|" Body="&lt;p&gt;How do I center?&lt;/p&gt;&lt;pre&gt;&lt;code&gt;div { margin: auto; }&lt;/code&gt;&lt;/pre&gt;" />\n',
        '  <row Id="101" PostTypeId="1" AcceptedAnswerId="201" Score="1" Title="Low score question" Tags="|python|" Body="&lt;p&gt;Low score.&lt;/p&gt;" />\n',
        '  <row Id="102" PostTypeId="1" Score="10" Title="No accepted answer" Tags="|java|" Body="&lt;p&gt;No answer.&lt;/p&gt;" />\n',
        '  <row Id="103" PostTypeId="1" AcceptedAnswerId="203" Score="8" Title="Python list comprehension" Tags="|python|" Body="&lt;p&gt;How to filter a list?&lt;/p&gt;&lt;img src=&quot;https://i.stack.imgur.com/abc12.png&quot; alt=&quot;example&quot;&gt;" />\n',
        '  <row Id="200" PostTypeId="2" ParentId="100" Score="12" Body="&lt;p&gt;Use &lt;code&gt;margin: auto&lt;/code&gt;.&lt;/p&gt;" />\n',
        '  <row Id="201" PostTypeId="2" ParentId="101" Score="3" Body="&lt;p&gt;Low score answer.&lt;/p&gt;" />\n',
        '  <row Id="203" PostTypeId="2" ParentId="103" Score="20" Body="&lt;p&gt;Use a list comprehension.&lt;/p&gt;&lt;pre&gt;&lt;code&gt;result = [x for x in lst if x &gt; 0]&lt;/code&gt;&lt;/pre&gt;" />\n',
        '</posts>\n',
    ]
    path = os.path.join(tmp_dir, 'Posts.xml')
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    return path
