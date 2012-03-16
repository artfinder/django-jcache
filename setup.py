# Use setuptools if we can
try:
    from setuptools.core import setup
except ImportError:
    from distutils.core import setup

PACKAGE = 'django-jcache'
VERSION = '0.5'

setup(
    name=PACKAGE, version=VERSION,
    description="JCache support for Django",
    packages=[ 'jcache' ],
    license='MIT',
    author='Art Discovery Ltd',
    maintainer='James Aylett',
    maintainer_email='james@tartarus.org',
    install_requires=[
        'Django>=1.3',
        'celery',
    ],
    # url = 'http://code.artfinder.com/projects/django-jcache/',
    classifiers=[
        'Intended Audience :: Developers',
        'Framework :: Django',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
    ],
)
