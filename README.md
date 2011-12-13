# Django JCache

A small Django plugin that provides a "JCache", ie a cache where stale but unexpired keys can be regenerated
asynchronously, avoiding a thundering herd. Currently does *not* avoid a startup herd, although this can be
improved (earlier versions by default blocked in the startup condition, waiting for the async generator to
complete).

Requires celery and a Django cache backend with atomic INCR and DECR. The redis backend will work (v0.9.2 or
later), or the memcached backend should work although this isn't tested.

# In transition

This started as internal code, so it's rough around the edges particularly with respect to documentation. Also,
it's structured as a Django app because that makes running its tests easier (and assists task discovery with
django-celery); the tests are further complicated by the fact that its tests require a fully-running celery to
pass.

# Contact

Via [the github project page](http://github.com/artfinder/django-jcache).
