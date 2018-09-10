from setuptools import setup

setup(
    name='imgserv',
    package_dir={'lsst': 'python/lsst'},
    package_data={'lsst': ['dax/imgserv/config/*', 'dax/imgserv/templates/*', 'dax/imgserv/static/*']},
    packages=['lsst', 'lsst.dax.imgserv', 'lsst.dax.imgserv.getimage'],
    zip_safe=False,
    use_scm_version=True
)
