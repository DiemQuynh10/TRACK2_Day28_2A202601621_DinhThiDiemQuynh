from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64

asker = Entity(name="asker", join_keys=["asker_id"], value_type=ValueType.STRING)

asker_activity_source = FileSource(
    name="asker_activity_source",
    path="../.lab28/delta/exports/asker_activity",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

asker_activity = FeatureView(
    name="asker_activity_v1",
    entities=[asker],
    ttl=timedelta(days=7),
    schema=[
        Field(name="feedback_count", dtype=Int64),
        Field(name="avg_rating", dtype=Float64),
        Field(name="negative_ratio", dtype=Float64),
        Field(name="delta_version", dtype=Int64),
    ],
    source=asker_activity_source,
    online=True,
)

asker_serving = FeatureService(
    name="asker_serving_v1",
    features=[asker_activity],
)
