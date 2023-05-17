import datetime
from typing import Union

from redisolar.models import MetricUnit

DEFAULT_KEY_PREFIX = "ru102py-test"


def prefixed_key(f):
    """
    A method decorator that prefixes return values.

    Prefixes any string that the decorated method `f` returns with the value of
    the `prefix` attribute on the owner object `self`.
    """
    def prefixed_method(self, *args, **kwargs):
        key = f(self, *args, **kwargs)
        return f"{self.prefix}:{key}"

    return prefixed_method


class KeySchema:
    """
    Methods to generate key names for Redis data structures.

    These key names are used by the DAO classes. This class therefore contains
    a reference to all possible key names used by this application.
    """
    def __init__(self, prefix: str = DEFAULT_KEY_PREFIX):
        self.prefix = prefix

    @prefixed_key
    def site_hash_key(self, site_id: Union[int, str]) -> str:
        """
        sites:info:[site_id]
        Redis type: hash
        """
        return f"sites:info:{site_id}"

    @prefixed_key
    def site_ids_key(self) -> str:
        """
        sites:ids
        Redis type: set
        """
        return "sites:ids"

    @prefixed_key
    def site_geo_key(self) -> str:
        """
        sites:geo
        Redis type: geo
        """
        return "sites:geo"

    @prefixed_key
    def site_stats_key(self, site_id: int, day: datetime.datetime) -> str:
        """
        sites:stats:[year-month-day]:[site_id]
        Redis type: sorted set
        """
        return f"sites:stats:{day.strftime('%Y-%m-%d')}:{site_id}"

    @prefixed_key
    def capacity_ranking_key(self):
        """
        sites:capacity:ranking
        Redis type: sorted set
        """
        return "sites:capacity:ranking"

    @prefixed_key
    def day_metric_key(self, site_id: int, unit: MetricUnit,
                       time: datetime.datetime) -> str:
        """
        metric:[unit-name]:[year-month-day]:[site_id]
        Redis type: sorted set
        """
        return f"metric:{unit.value}:{time.strftime('%Y-%m-%d')}:{site_id}"

    @prefixed_key
    def global_feed_key(self) -> str:
        """
        sites:feed
        Redis type: stream
        """
        return "sites:feed"

    @prefixed_key
    def feed_key(self, site_id: int) -> str:
        """
        sites:feed:[site_id]
        Redis type: stream
        """
        return f"sites:feed:{site_id}"

    @prefixed_key
    def fixed_rate_limiter_key(self, name: str, minute_block: int, max_hits: int) -> str:
        """
        limiter:[name]:[duration]:[max_hits]
        Redis type: string of type integer
        """
        return f"limiter:{name}:{minute_block}:{max_hits}"

    @prefixed_key
    def sliding_window_rate_limiter_key(self, name: str, window_size_ms: int,
                                        max_hits: int) -> str:
        """
        limiter:[name]:[window_size_ms]:[max_hits]
        Redis type: string of type integer
        """
        return f"limiter:{name}:{window_size_ms}:{max_hits}"

    @prefixed_key
    def timeseries_key(self, site_id: int, unit: MetricUnit) -> str:
        return f"sites:ts:{site_id}:{unit.value}"


    # Test keys
    @prefixed_key
    def planets_list_key(self) -> str:
        return "planets:list"

    @prefixed_key
    def planets_set_key(self) -> str:
        return "planets:set"

    @prefixed_key
    def hello_key(self) -> str:
        return "hello"

    @prefixed_key
    def quiz_get_set_key(self) -> str:
        return "quiz:get-set"

    @prefixed_key
    def quiz_get_members_key(self) -> str:
        return "quiz:get-members"

    @prefixed_key
    def quiz_metrics_key(self) -> str:
        return "quiz:metrics"

    @prefixed_key
    def quiz_pipeline_key_1(self) -> str:
        return "quiz:pipeline1"

    @prefixed_key
    def quiz_pipeline_key_2(self) -> str:
        return "quiz:pipeline2"

    @prefixed_key
    def quiz_streams_key(self) -> str:
        return "quiz:streams"

    @prefixed_key
    def quiz_race_condition_key(self) -> str:
        return "quiz:race-condition"

    @prefixed_key
    def quiz_rate_limiter_key(self, epoch_seconds, user_id) -> str:
        return f"quiz:limiter:{epoch_seconds}-{user_id}"
