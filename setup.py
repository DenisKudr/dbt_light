from setuptools import setup, find_packages


setup(name='dbt_light',
      version='0.0.1',
      description='Lightweight data build tool',
      packages=find_packages(),
      install_requires=['PyYAML>=6.0', 'Jinja2>=3.0.3', 'psycopg2>=2.9.3', 'schema>=0.7.5'],
      author='Denis Kudryavtsev',
      author_email='denizkudryavtsev@yandex.ru',
      zip_safe=False,
      python_requires=">=3.8.10",
      include_package_data=True
      )
