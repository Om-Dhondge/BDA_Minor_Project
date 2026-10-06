"""Cypher for stage 5. Every query is parameterized: tag names arrive from the
command line and are never formatted into query text.

These connect and rank TECHNOLOGIES. The synopsis's user-centric query -
"users who bridge multiple domains" - stays blocked on OwnerUserId; BRIDGE_TAGS
is its tag-level counterpart.
"""

EXPECTED_COUNTS = {
    "Tag nodes": 49_372,
    "Question nodes": 2_532_514,
    "TAGGED_WITH relationships": 7_775_852,
    "CO_OCCURS relationships": 1_469_065,
    "Tags without metrics": 23_775,
}

COUNT_QUERIES = {
    "Tag nodes": "MATCH (t:Tag) RETURN count(t) AS n",
    "Question nodes": "MATCH (q:Question) RETURN count(q) AS n",
    "TAGGED_WITH relationships": "MATCH ()-[r:TAGGED_WITH]->() RETURN count(r) AS n",
    "CO_OCCURS relationships": "MATCH ()-[r:CO_OCCURS]->() RETURN count(r) AS n",
    # Proves empty CSV fields became absent properties rather than zeros.
    "Tags without metrics": "MATCH (t:Tag) WHERE t.community IS NULL RETURN count(t) AS n",
}

# Backs every {name: $tag} lookup below. The bulk importer creates no indexes.
SCHEMA = [
    "CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE",
]

# Objective 4's own example: how a niche connects to the rest of the graph.
SHORTEST_PATH = """
MATCH (a:Tag {name: $source}), (b:Tag {name: $target})
MATCH p = shortestPath((a)-[:CO_OCCURS*..6]-(b))
WHERE all(r IN relationships(p) WHERE r.weight >= $min_weight)
RETURN [n IN nodes(p) | n.name] AS path,
       [n IN nodes(p) | n.community] AS communities,
       [r IN relationships(p) | r.weight] AS weights
"""

BRIDGE_TAGS = """
MATCH (t:Tag)-[r:CO_OCCURS]-(n:Tag)
WHERE r.weight >= $min_weight
  AND t.community IS NOT NULL AND n.community IS NOT NULL
  AND n.community <> t.community
RETURN t.name AS tag, t.community AS community,
       count(DISTINCT n.community) AS other_communities,
       sum(r.weight) AS cross_weight
ORDER BY other_communities DESC, cross_weight DESC
LIMIT $limit
"""

TAGGED_BOTH = """
MATCH (a:Tag {name: $tag_a})<-[:TAGGED_WITH]-(q:Question)-[:TAGGED_WITH]->(b:Tag {name: $tag_b})
RETURN q.question_id AS question_id, q.score AS score, q.answer_count AS answers
ORDER BY score DESC
LIMIT $limit
"""

# Ranked by the weaker of the two links, so a tag tied strongly to only one
# side of the pair does not float to the top.
STACK_COMPLETION = """
MATCH (a:Tag {name: $tag_a})-[r1:CO_OCCURS]-(c:Tag)-[r2:CO_OCCURS]-(b:Tag {name: $tag_b})
WITH c, r1.weight AS with_a, r2.weight AS with_b
RETURN c.name AS tag, with_a, with_b,
       CASE WHEN with_a < with_b THEN with_a ELSE with_b END AS weaker_link
ORDER BY weaker_link DESC
LIMIT $limit
"""
