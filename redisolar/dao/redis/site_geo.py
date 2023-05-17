from typing import List
from typing import Set

import redis.client

from redisolar.dao.base import SiteGeoDaoBase
from redisolar.dao.base import SiteNotFound
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.models import GeoQuery, MeterReading
from redisolar.models import Site
from redisolar.schema import FlatSiteSchema, MeterReadingSchema

CAPACITY_THRESHOLD = 0.2


class SiteGeoDaoRedis(SiteGeoDaoBase, RedisDaoBase):
    """SiteGeoDaoRedis persists and queries Sites in Redis."""

    def insert(self, site: Site, **kwargs):
        """Insert a Site into Redis."""
        hash_key = self.key_schema.site_hash_key(site.id)
        client = kwargs.get('pipeline', self.redis)
        client.hset(hash_key, mapping=FlatSiteSchema().dump(site))  # type: ignore

        if not site.coordinate:
            raise ValueError("Site coordinates are required for Geo insert")

        client.geoadd(
            self.key_schema.site_geo_key(), (site.coordinate.lng, site.coordinate.lat, site.id))

    def _insert(self, meter_reading: MeterReading, pipeline: redis.client.Pipeline) -> None:
        global_key = self.key_schema.global_feed_key()
        site_key = self.key_schema.feed_key(meter_reading.site_id)
        serialized_meter_reading = MeterReadingSchema().dump(meter_reading)

        pipeline.xadd(global_key, serialized_meter_reading, maxlen=self.GLOBAL_MAX_FEED_LENGTH)
        pipeline.xadd(site_key, serialized_meter_reading, maxlen=self.SITE_MAX_FEED_LENGTH)

    def insert_many(self, *sites: Site, **kwargs) -> None:
        """Insert multiple Sites into Redis."""
        for site in sites:
            self.insert(site, **kwargs)

    def find_by_id(self, site_id: int, **kwargs) -> Site:
        """Find a Site by ID in Redis."""
        hash_key = self.key_schema.site_hash_key(site_id)
        site_hash = self.redis.hgetall(hash_key)

        if not site_hash:
            raise SiteNotFound()

        return FlatSiteSchema().load(site_hash)

    def _find_by_geo(self, query: GeoQuery, **kwargs) -> Set[Site]:
        site_ids = self.redis.georadius(  # type: ignore
            self.key_schema.site_geo_key(), query.coordinate.lng, query.coordinate.lat, query.radius,
            query.radius_unit.value)
        sites = [self.redis.hgetall(self.key_schema.site_hash_key(site_id)) for site_id in site_ids]
        return {FlatSiteSchema().load(site) for site in sites}

    def _find_by_geo_with_capacity(self, query: GeoQuery, **kwargs) -> Set[Site]:
        coord = query.coordinate
        site_ids: List[str] = self.redis.georadius(
            self.key_schema.site_geo_key(), coord.lng, coord.lat, query.radius,
            query.radius_unit.value)

        if not site_ids:
            return set()

        p = self.redis.pipeline(transaction=False)
        for site_id in site_ids:
            p.zscore(self.key_schema.capacity_ranking_key(), site_id)
        scores = dict(zip(site_ids, p.execute()))

        for site_id in site_ids:
            score = scores.get(site_id) or 0.0
            if score > CAPACITY_THRESHOLD:
                p.hgetall(self.key_schema.site_hash_key(site_id))

        return {FlatSiteSchema().load(site) for site in p.execute()}

    def find_by_geo(self, query: GeoQuery, **kwargs) -> Set[Site]:
        """Find Sites using a geographic query."""
        if query.only_excess_capacity:
            return self._find_by_geo_with_capacity(query)
        return self._find_by_geo(query)

    def find_all(self, **kwargs) -> Set[Site]:
        """Find all Sites."""
        site_ids = self.redis.zrange(self.key_schema.site_geo_key(), 0, -1)
        if len(site_ids) < 1:
            return set()

        pipeline = self.redis.pipeline(transaction=False)

        for site_id in site_ids:
            key = self.key_schema.site_hash_key(site_id)
            pipeline.hgetall(key)

        return set(FlatSiteSchema().load(site_hash) for site_hash in pipeline.execute())
