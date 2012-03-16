import time
import logging
from django.conf import settings
from django.core.cache.backends.base import BaseCache
from django.core.cache import get_cache as get_django_cache, DEFAULT_CACHE_ALIAS
from django.utils.functional import SimpleLazyObject
from celery.task import task


logger = logging.getLogger(__name__)
#handler = logging.StreamHandler()
#handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
#logger.addHandler(handler)
#logger.setLevel(logging.INFO)


@task
def invoke_async(jcache, key, version, generator, stale, args, kwargs):
    try:
        logger = invoke_async.get_logger()
        #kwargs['logger'] = logger
        logger.info("running generator %s" % generator)
        value = generator(*args, **kwargs)
        if stale is None:
            stale_at = time.time() + jcache.stale
        else:
            stale_at = time.time() + stale
        #logger.info("setting %s = %s (%s/%s)" % (key, value, stale_at, jcache.expiry))
        logger.info("setting key %s (%s/%s)" % (key, stale_at, jcache.expiry))
        jcache.set(
            key,
            value=value,
            stale_at=stale_at,
            timeout=jcache.expiry,
            version=version,
            )
        jcache._decr_flag(key, version=version)
    except:
        jcache._decr_flag(key, version=version)
        raise
    return value


class JCache(object):
    """
    JCache, ie a cache that behaves somewhat like varnish in that as well
    as keys having the states missing/fresh/expired there is a fourth state
    in the lifecycle, stale, between fresh & expired.

    Uses Celery to make an async rebuild request, and uses a second
    cache key with incr to prevent the async task from being scheduled
    twice. You really only want to use this with underlying cache
    backends that make incr atomic (memcache does, redis does with
    Sean's feature/native-incr branch).
    """

    def __init__(self, stale=240, expiry=None, cache=None):
        """
        `stale` is the number of seconds before 
        """
        if isinstance(cache, basestring):
            self._cache = get_django_cache(cache)
        elif isinstance(cache, BaseCache):
            self._cache = cache
        elif cache is None:
            self._cache = get_django_cache(DEFAULT_CACHE_ALIAS)
        else:
            raise TypeError("cache parameter must be None, the name of a Django cache, or a BaseCache object")
        self.stale = stale
        self.expiry = expiry

    def get(self,
            key,
            version=None,
            stale=None,
            generator=None,
            wait_on_generate=False,
            *args, **kwargs):
        """
        Get a value by key, with optional versioning (see Django 1.3 docs).
        If you supply `generator`, it will be called with `*args, **kwargs`
        as you might expect. All arguments including the generator must
        be capable of being pickled (this means that the generator has to
        be at top level in a module from a fully-qualified import).

        `stale` overrides the cache's default.

        If `generator` is None and the key isn't already in the cache,
        you get None. So this is a bit like a dynamic, lazy version of
        the `default` parameter to a normal Django cache's `get()` method.

        If `generator` is passed, and `wait_on_generate` is False (the
        default) then the generator will be called (asynchronously)
        but you'll still get back None if the key wasn't in the JCache
        to start off with.

        If `generator` is passed and `wait_on_generate` is True, this
        call blocks on the asynchronous generation. You can turn this
        into non-blocking by setting `_jcache_options` on the
        generator as a dictionary with member `lazy_result` set to
        True, in which case we return a `SimpleLazyObject` which will
        block on the getting the result from the asynchronous
        generator when the lazy object is resolved. Note that for this
        to work, the result will be passed back from celery, meaning
        it must be picklable.
        """

        if isinstance(key, list) or isinstance(key, tuple):
            # conflate them with '-' to make the key
            key = '-'.join(map(lambda x: unicode(x).encode('utf-8'), key))

        flag = self._incr_flag(key, version)
        do_decr = True
        try:
            packed = self._cache.get(
                "data:%s" % key,
                default=None,
                version=version
                )
            generate = False
            if packed is None:
                generate = generator is not None
                if wait_on_generate:
                    # we'll get a startup herd here; another approach is to
                    # instead wait until there's a value, but that's harder
                    flag = 1
                value = None
            else:
                (value, stale_at) = packed
                now = time.time()
                if stale_at < now:
                    generate = True

            logger.info('jcache: %s=%s, generate=%s', key, packed, generate)
            
            if generate and generator is not None and flag == 1:
                do_decr = False
                logger.info('jcache (async) generating...')
                result = invoke_async.delay(
                    self,
                    key,
                    version,
                    generator,
                    stale,
                    args,
                    kwargs
                    )
                if packed is None:
                    if not wait_on_generate:
                        logger.info("not waiting for %s", key)
                        value = None
                    else:
                        logger.info("waiting for %s", key)
                        if hasattr(generator, '_jcache_options'):
                            opts = generator._jcache_options
                        else:
                            opts = {}
                        if opts.get('lazy_result', False):
                            # note that resolve may be called multiple times
                            # if the SimpleLazyObject is deepcopied.
                            def resolve():
                                return result.get(propagate=True)
                            value = SimpleLazyObject(resolve)
                        else:
                            value = result.get(propagate=True)
                        logger.info("got the key %s = %s", key, value)
        finally:
            if do_decr:
                self._decr_flag(key, version)

        return value

    def _incr_flag(self, key, version):
        try:
            v = self._cache.incr("flag:%s" % key, version=version)
            #print "_incr_flag ->", v
            return v
        except ValueError:
            self._cache.set("flag:%s" % key, 1, version=version)
            #print "_incr_flag ->", 1
            return 1

    def _decr_flag(self, key, version):
        v = self._cache.decr("flag:%s" % key, version=version)
        #print "_decr_flag ->", v
        return v

    def _reset_flag(self, key, version):
        self._cache.set("flag:%s" % key, 0, version=version)

    def set(self,
            key,
            value=None,
            stale_at=None,
            version=None,
            timeout=None
            ):
        if timeout is None:
            timeout = self.expiry
        if stale_at is None:
            stale_at = time.time() + self.stale
        return self._cache.set(
            "data:%s" % key,
            (value, stale_at),
            timeout=timeout,
            version=version,
            )

    def delete(self, key, version):
        self._cache.delete(key, version=version)

    def clear(self):
        self._cache.clear()


if not hasattr(settings, 'JCACHES'):
    settings.JCACHES = { DEFAULT_CACHE_ALIAS: { }, }


_jcaches = {}
def get_cache(name):
    if name not in _jcaches:
        #print "fetching", name
        config = settings.JCACHES.get(name)
        if config is None:
            #print "no config!", settings.JCACHES
            raise ValueError()
        else:
            #print "got config", config
            pass
        _jcaches[name] = JCache(**config)
    return _jcaches[name]


cache = get_cache(DEFAULT_CACHE_ALIAS)
