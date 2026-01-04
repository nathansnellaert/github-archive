import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_matches_pattern, assert_positive


def test(table: pa.Table) -> None:
    """Validate GitHub activity output."""
    validate(table, {
        "columns": {
            "hour": "string",
            "event_type": "string",
            "event_count": "int",
        },
        "not_null": ["hour", "event_type", "event_count"],
        "unique": ["hour", "event_type"],
        "min_rows": 1,
    })

    # Hour format check
    assert_matches_pattern(table, "hour", r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00Z$", "hourly timestamp")

    # Event counts must be positive
    assert_positive(table, "event_count", allow_zero=False)

    # Known GitHub event types
    known_types = {
        "PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent",
        "ForkEvent", "CreateEvent", "DeleteEvent", "IssueCommentEvent",
        "PullRequestReviewEvent", "PullRequestReviewCommentEvent",
        "CommitCommentEvent", "GollumEvent", "MemberEvent", "PublicEvent",
        "ReleaseEvent", "SponsorshipEvent"
    }
    event_types = set(table.column("event_type").to_pylist())
    unknown = event_types - known_types
    if unknown:
        print(f"    Note: Found new event types: {unknown}")
