import time
import django.utils.unittest as unittest
from django.test import TestCase
from django.core.cache.backends.locmem import LocMemCache
from django.core.cache.backends.filebased import FileBasedCache
from django.conf import settings

from jcache import JCache


CACHES = {
    'default': LocMemCache('unique-snowflake', { 'TIMEOUT': 1 }),
    'secondary': LocMemCache('second-unique-snowflake', {}),
    'file-backed': FileBasedCache('/var/tmp/django_jcache_tests', {}),
}


JCACHES = {
    # one will never go into stale, because the entries TIMEOUT too fast
    'one': JCache(stale=2, expiry=None, cache=CACHES['default']),
    # two will pass through stale because we override TIMEOUT by
    # having a higher expiry
    'two': JCache(stale=2, expiry=3, cache=CACHES['default']),
    # secondary doesn't have a TIMEOUT (so it's the default of 300),
    # but we override anyway to keep things down
    'three': JCache(stale=2, expiry=3, cache=CACHES['secondary']),
    # just like three, but uses a backend that can be pickled
    'async': JCache(stale=2, expiry=3, cache=CACHES['file-backed']),
    # and one without expiry
    'async-noexpire': JCache(stale=2, expiry=None, cache=CACHES['file-backed']),
}


def simple_build(*args, **kwargs):
    return "result"


def delayed_build(*args, **kwargs):
    time.sleep(2)
    return "result"


def failed_build(*args, **kwargs):
    raise Exception("message")


def param_build(*args, **kwargs):
    assert kwargs['param1']==1 and kwargs['param2']==2
    return "result"


def param_build2(param1, *args, **kwargs):
    return param1


def enumerating_build(*args, **kwargs):
    c = CACHES['file-backed']
    #kwargs['logger'].info("running, count = %i" % c.get('build-count'))
    c.incr('build-count')
    #kwargs['logger'].info("and now count = %i" % c.get('build-count'))
    time.sleep(2)
    return "result"


class TestJCache(TestCase):
    """
    Various basic tests of JCache functionality.
    """
    
    def tearDown(self):
        for cache in CACHES.values():
            cache.clear()
    
    def test_preinsert(self):
        jc = JCACHES["one"]
        self.assertEqual(None, jc.get("cachekey"))
    
    def test_insert_timeout_before_stale(self):
        jc = JCACHES["one"]
        
        self.assertEqual("result", jc.get("cachekey", generator=simple_build, wait_on_generate=True))
        time.sleep(2)
        self.assertEqual(None, jc.get("cachekey")) # because has passed TIMEOUT
    
    def test_insert_stale_timeout_expiry(self):
        jc = JCACHES["two"]
        
        def build(*args, **kwargs):
            return "result"
        
        self.assertEqual("result", jc.get("cachekey", generator=simple_build, wait_on_generate=True))
        time.sleep(2)
        self.assertEqual("result", jc.get("cachekey")) # is stale but not expired
        time.sleep(1)
        self.assertEqual(None, jc.get("cachekey")) # has passed expiry
    
    def test_insert_stale_expiry_timeout(self):
        jc = JCACHES["three"]

        self.assertEqual("result", jc.get("cachekey", generator=simple_build, wait_on_generate=True))
        time.sleep(2)
        self.assertEqual("result", jc.get("cachekey")) # is stale but not expired
        time.sleep(1)
        self.assertEqual(None, jc.get("cachekey")) # has passed expiry

    def test_failed_build(self):
        jc = JCACHES["one"]
        try:
            jc.get("cachekey", generator=failed_build, wait_on_generate=True)
            self.fail("Should raise an exception")
        except Exception, e:
            self.assertEqual("message", str(e))

    def test_params(self):
        jc = JCACHES["one"]
        self.assertEqual(
            "result",
            jc.get("cachekey", generator=param_build, wait_on_generate=True, param1=1, param2=2)
            )


class TestJCacheAsyncRegen(TestCase):
    """
    JCache tests that need an async driver. This needs to be *actually*
    async, ie you can't run with CELERY_ALWAYS_EAGER (which is common to
    do during test runs). So we need to shuffle that out of the way.
    This means that RUNNING TESTS WILL RESULT IN REAL TASKS ON YOUR
    CELERY TASK QUEUE.
    """

    @unittest.skipIf(not hasattr(settings, 'TESTS_ACTIVE_CELERY') or not \
                settings.TESTS_ACTIVE_CELERY, "no activey celery configuration")
    def setUp(self):
        if hasattr(settings, 'CELERY_ALWAYS_EAGER'):
            self.eagerness = settings.CELERY_ALWAYS_EAGER
        else:
            self.eagerness = False # default
        settings.CELERY_ALWAYS_EAGER = False

    def tearDown(self):
        settings.CELERY_ALWAYS_EAGER = self.eagerness
        for cache in CACHES.values():
            cache.clear()
    
    def test_async_regen_on_stale(self):
        jc = JCACHES["async"]
        
        self.assertEqual("result", jc.get("cachekey", generator=delayed_build, wait_on_generate=True))
        time.sleep(2)
        then = time.time()
        self.assertEqual("result", jc.get("cachekey", generator=delayed_build, wait_on_generate=True)) # is stale but not expired
        self.assertTrue(then + 1 > time.time()) # ie can't have done the rebuild async
        time.sleep(3) # wait a bit longer so async actually happens
        self.assertEqual("result", jc.get("cachekey")) # original has passed expiry, but has been regened

    def test_no_wait_on_generate(self):
        # if we don't wait on generation, we'll get None from get the
        # first time we call with a generator.
        jc = JCACHES["async"]
        
        self.assertEqual(None, jc.get("cachekey", generator=simple_build))
        time.sleep(2)
        then = time.time()
        self.assertEqual("result", jc.get("cachekey")) # is stale but not expired

    def test_async_runs_once(self):
        jc = JCACHES["async"]
        c = CACHES['file-backed']
        c.set('build-count', 0)
        now = time.time()
        c.set('data:cachekey', ('initial', now-2), version=None)
        self.assertEqual('initial', c.get('data:cachekey')[0])
        # key is already stale, so if we request twice in a row we should get
        # initial both times, then we sleep a couple of seconds and we should
        # get result it having been generated once (build-count==1)
        self.assertEqual('initial', jc.get('cachekey', generator=enumerating_build, wait_on_generate=True))
        self.assertEqual('initial', jc.get('cachekey', generator=enumerating_build, wait_on_generate=True))
        # wait for it to be generated
        time.sleep(3)
        self.assertEqual('result', jc.get('cachekey', generator=enumerating_build, wait_on_generate=True))
        self.assertEqual(1, c.get('build-count'))

    def test_regression_1(self):
        # we weren't decrementing the flag if we didn't regenerate
        # causing multiple hits before stale to end up with a non-zero
        # flag
        jc = JCACHES["async-noexpire"]
        self.assertEqual('result', jc.get("cachekey", generator=param_build2, wait_on_generate=True, param1='result'))
        self.assertEqual('result', jc.get("cachekey", generator=param_build2, wait_on_generate=True, param1='result'))
        # without decr, at this point the flag would be non-zero
        time.sleep(2)
        self.assertEqual('result', jc.get("cachekey", generator=param_build2, wait_on_generate=True, param1='next'))
        time.sleep(1)
        self.assertEqual('next', jc.get("cachekey", generator=param_build2, wait_on_generate=True, param1='other'))

    def test_async_failed_build(self):
        # test that if an async build fails, the flag gets decremented to 0
        jc = JCACHES["async"]
        c = CACHES['file-backed']
        
        self.assertEqual(None, jc.get("cachekey", generator=failed_build))
        self.assertEqual(1, c.get("flag:cachekey"))
        time.sleep(2)
        self.assertEqual(0, c.get("flag:cachekey"))

