"""Shape graph artifacts into CSVs for `neo4j-admin database import full`.

pandas, not Spark: this is a format conversion over artifacts that already
exist. Spark writes a directory of part files, each with its own header line,
and the importer reads every line after the first as data.

Column names ARE the importer's header syntax: `name:ID(Tag)` declares the id
column of the Tag id space, `:START_ID(Question)` a relationship's start node,
`weight:long` a typed property.
"""
import pandas as pd

METRIC_COLUMNS = ["tag_authority", "community", "component", "triangle_count"]

# Only the 25,597 tags that survive weight >= 5 have metrics, so these columns
# carry nulls. As float64 they print 180388626595.0, which the importer
# rejects for a long property - and LPA labels reach ~4e11. Int64 is pandas'
# nullable integer: whole numbers print bare, nulls print as empty fields.
NULLABLE_INT_METRICS = ["community", "component", "triangle_count"]

TAG_HEADER = {
    "tag": "name:ID(Tag)",
    "question_count": "question_count:long",
    "mean_question_score": "mean_question_score:double",
    "mean_answers_per_question": "mean_answers_per_question:double",
    "mean_answer_score": "mean_answer_score:double",
    "tag_authority": "tag_authority:double",
    "community": "community:long",
    "component": "component:long",
    "triangle_count": "triangle_count:long",
}

QUESTION_HEADER = {
    "question_id": "question_id:long",
    "score": "score:long",
    "answer_count": "answer_count:long",
    "mean_answer_score": "mean_answer_score:double",
    "tag_count": "tag_count:long",
}


def tag_nodes(tags: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    """Every tag, with the primary-threshold metrics where it has them."""
    merged = tags.merge(metrics[["tag", *METRIC_COLUMNS]], on="tag", how="left")
    for column in NULLABLE_INT_METRICS:
        merged[column] = merged[column].astype("Int64")
    return merged[list(TAG_HEADER)].rename(columns=TAG_HEADER)


def question_nodes(questions: pd.DataFrame) -> pd.DataFrame:
    """Question nodes keyed by an unnamed id column.

    An unnamed :ID column is used to match relationships but not stored. A
    named one would be stored as a string, because tag names force the
    importer's global id type to string - so question_id is stored separately
    as a long.
    """
    nodes = questions[list(QUESTION_HEADER)].rename(columns=QUESTION_HEADER)
    nodes.insert(0, ":ID(Question)", questions["question_id"].to_numpy())
    return nodes


def tagged_with_rels(edges: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        ":START_ID(Question)": edges["question_id"].to_numpy(),
        ":END_ID(Tag)": edges["tag"].to_numpy(),
    })


def co_occurs_rels(edges: pd.DataFrame) -> pd.DataFrame:
    """One relationship per pair, src < dst, as stage 2 stores it.

    Cypher matches CO_OCCURS undirected, so reverse edges would only double
    every count and weight sum.
    """
    return pd.DataFrame({
        ":START_ID(Tag)": edges["src"].to_numpy(),
        ":END_ID(Tag)": edges["dst"].to_numpy(),
        "weight:long": edges["weight"].to_numpy(),
    })


def to_import_csv(frame: pd.DataFrame, path) -> int:
    """Write one header line, then data. Nulls become empty fields, which the
    importer leaves as absent properties."""
    frame.to_csv(path, index=False, na_rep="")
    return len(frame)
