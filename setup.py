import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nhlscrapy",
    version="0.0.1",
    author="Xavier Thierry",
    author_email="xaverthierry0@gmail.com",
    description="A fast downloader of NHL data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xaaaaav/nhlscrapy",
    packages=setuptools.find_packages(),
    license = "http://www.apache.org/licenses/LICENSE-2.0"
)