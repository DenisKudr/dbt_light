from pathlib import Path
from setuptools import setup, find_packages

this_directory = Path(__file__).parent
long_description = Path(this_directory, 'README.md').read_text()

setup(name='dbt-light',
      version='1.0.0',
      description='Lightweight data build tool',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url="https://github.com/DenisKudr/dbt_light",
      packages=find_packages(),
      install_requires=Path("./requirements.txt").read_text().splitlines(),
      author='Denis Kudryavtsev',
      author_email='denizkudryavtsev@yandex.ru',
      license='Apache License 2.0',
      zip_safe=False,
      python_requires=">=3.8.10",
      include_package_data=True
      )
