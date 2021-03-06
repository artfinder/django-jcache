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
def invoke_async(jcache, key, version, generator, stale, args, kwargs, expires_at=None):
    value = None
    
    try:
        logger = invoke_async.get_logger()
        
        if expires_at is None or expires_at > time.time():
            logger.debug("running generator %s" % generator)
            
            value = generator(*args, **kwargs)
            
            stale_at = time.time() + (stale or jcache.stale)
            
            logger.debug("setting key %s (%s/%s)" % (key, stale_at, jcache.expiry))
            jcache.set(
                key,
                value=value,
                stale_at=stale_at,
                timeout=jcache.expiry,
                version=version,
            )
        else:
            logger.debug('invoke_async (%s) expired while waiting for worker' % generator)
    finally:
        flag = jcache._decr_flag(key, version, 1 + (stale or jcache.stale))

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
            async_wait_on_generate=False,
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

        If `async_wait_on_generate` is True then invoke_async will be
        executed via Celery. Else it will be invoked in the current
        thread instead.
        """

        if isinstance(key, list) or isinstance(key, tuple):
            # conflate them with '-' to make the key
            key = '-'.join(map(lambda x: unicode(x).encode('utf-8'), key))

        flag = None
        value = None
        do_decr = False
        generate = False
        async_result = None
        now = time.time()
            
        packed = self._cache.get(
            "data:%s" % key,
            default=None,
            version=version
            )
        
        # no data for this key, we'll want to try and generate some
        if packed is None:
            generate = generator is not None
            if wait_on_generate:
                logger.warning("jcache: Assuming flag:%s=1 due to missing data and wait_on_generate" % key)
                # we'll get a startup herd here; another approach is to
                # instead wait until there's a value, but that's harder
                # we'd need to store the task ID of the active invoke_async for this key in redis so we can make a
                # AsyncResult here and wait for it to complete
                flag = 1
            value = None
        else:
            (value, stale_at) = packed
            if stale_at < now:
                generate = generator is not None
    
        try:
            # we only need to know the flag value if we are thinking about
            # regenerating the key
            if generate:
                flag = self._incr_flag(key, version, 1 + (stale or self.stale))
                do_decr = True
                if flag < 1: # flag was <= -1 before incr, happens when flag expires just before decr was called
                    logger.warning('jcache: key=%s flag=%s resetting to 1', key, flag)
                    flag = self._reset_flag(key, version, 1 + (stale or self.stale), value=1)

                logger.info('jcache: %s=%s, generate=%s flag=%s', key, packed, generate, flag)
               
                # only generate a value if we are the only active instance
                if generate and flag == 1:
                    invoke_async_args = (
                        self,
                        key,
                        version,
                        generator,
                        stale,
                        args,
                        kwargs,
                        time.time() + (stale or self.stale)
                    )
                    
                    # async invocation is desired
                    if not wait_on_generate or (wait_on_generate and async_wait_on_generate):
                        do_decr = False # let invoke_async decrement the flag
                        logger.info('jcache: apply_sync generating data:%s' % key)
                        async_result = invoke_async.apply_async(args=invoke_async_args)

                    # block until we have a fresh value
                    if wait_on_generate and packed is None:
                        logger.info("jcache: waiting for data:%s", key)
                        
                        opts = getattr(generator, '_jcache_options', {})
                        
                        if async_result:
                            result_func = lambda: async_result.get(propogate=True)
                        else:
                            do_decr = False # invoke_async being called locally still decrements the flag
                            result_func = lambda: invoke_async(*invoke_async_args)

                        if opts.get('lazy_result'):
                            value = SimpleLazyObject(lambda: result_func())
                        else:
                            value = result_func() 
        finally:
            if do_decr:
                self._decr_flag(key, version, 1 + (stale or self.stale))

        return value

    def _incr_flag(self, key, version, timeout=None):
        try:
            return self._cache.incr("flag:%s" % key, version=version)
        except ValueError:
            return self._reset_flag(key, version, timeout, 1)

    def _decr_flag(self, key, version, timeout=None):
        try:
            return self._cache.decr("flag:%s" % key, version=version)
        except ValueError:
            return self._reset_flag(key, version, timeout, 0)

    def _reset_flag(self, key, version, timeout=None, value=0):
        self._cache.set("flag:%s" % key, value, version=version, timeout=timeout)
        return value

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

    def freshen(self, key, version=None, generator=None, stale=None, *args, **kwargs):
        flag = self._incr_flag(key, version, 1 + (stale or self.stale))
       
        if flag > 1:
            self._decr_flag(key, version)
        else:
            logger.warning('jcache: (freshen) key=%s flag=%s resetting to 1', key, flag)
            flag = self._reset_flag(key, version, 1 + (stale or self.stale), value=1)
        
        if flag == 1:
            return invoke_async.apply_async(
                args=(self, key, version, generator, stale, args, kwargs, time.time() + (stale or self.stale)),
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
