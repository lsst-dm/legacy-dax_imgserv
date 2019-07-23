from setuptools import setup

setup(
    name='imgserv',
    package_dir={'lsst': 'python/lsst', 'etc': 'rootfs/etc'},
    package_data={'lsst': ['dax/imgserv/config/*', 'dax/imgserv/templates/*',
                           'dax/imgserv/static/*']},
    packages=['lsst', 'etc', 'lsst.dax.imgserv',
              'lsst.dax.imgserv.getimage', 'lsst.dax.imgserv.vo',
              'lsst.dax.imgserv.vo.dal', 'lsst.dax.imgserv.vo.soda',
              'lsst.dax.imgserv.jobqueue', 'etc.celery'],
    zip_safe=False,
    use_scm_version={'version_scheme': 'post-release'},
    setup_requires=['setuptools_scm', 'pytest-flask', 'pytest-runner'],
    tests_require=['pytest'],
)
