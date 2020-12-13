from setuptools import find_packages, setup

test_requirements = ["pytest"]
docs_requirements = [
    "Sphinx==2.4.2",
    "sphinxcontrib-websupport==1.2.0",
    "sphinx_rtd_theme",
]
sql_requirements = ["SQLAlchemy>=1.3.18"]

setup(
    name="accsr",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    version="0.1.8-dev4",
    description="Utils for accessing data from anywhere",
    install_requires=open("requirements.txt").readlines(),
    setup_requires=["wheel"],
    tests_require=test_requirements,
    extras_require={
        "test": test_requirements,
        "docs": docs_requirements,
        "sql": sql_requirements,
    },
    author="AppliedAI",
)
