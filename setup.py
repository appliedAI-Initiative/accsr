from setuptools import find_packages, setup

test_requirements = ["pytest"]
docs_requirements = [
    "Sphinx==3.2.1",
    "sphinxcontrib-websupport==1.2.4",
    "sphinx_rtd_theme",
    "nbsphinx",
    "ipython",
]

setup(
    name="accsr",
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    version="0.3.4",
    description="Utils for accessing data from anywhere",
    install_requires=open("requirements.txt").readlines(),
    setup_requires=["wheel"],
    tests_require=test_requirements,
    extras_require={
        "test": test_requirements,
        "docs": docs_requirements,
    },
    author="AppliedAI",
)
