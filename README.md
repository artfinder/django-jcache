# Django JCache

A small Django plugin that provides a "JCache", ie a cache where stale but unexpired keys can be regenerated
asynchronously, avoiding a thundering herd. Currently does *not* avoid a startup herd, although this can be
improved (earlier versions by default blocked in the startup condition, waiting for the async generator to
complete).

# Contact

Via [the github project page](http://github.com/artfinder/django-jcache).
