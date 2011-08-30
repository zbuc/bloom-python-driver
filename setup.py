from setuptools import setup, Command
import pybloomd

# Get the long description by reading the README
try:
    readme_content = open("README.rst").read()
except:
    readme_content = ""

# Create the actual setup method
setup(name='pybloomd',
      version=pybloomd.__version__,
      description='Client library to interface with multiple bloomd servers',
      long_description=readme_content,
      author='Armon Dadgar',
      author_email='armon@kiip.me',
      maintainer='Armon Dadgar',
      maintainer_email='armon@kiip.me',
      url="https://github.com/kiip/pybloomd/",
      license="MIT License",
      keywords=["bloom", "filter","client","bloomd"],
      py_modules=['pybloom'],
      classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries",
    ]
      )
