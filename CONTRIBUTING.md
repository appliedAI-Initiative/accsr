accsr library
=============

## Installing

You should install the library together with all dependencies as an editable package. We strongly suggest to use some form of virtual environment for working with the library. E.g. with conda:

```shell
conda create -n accsr python=3.8
conda activate accsr
pip install -e .
pip install -r requirements-dev.txt -r requirements-docs.txt -r requirements-test.txt -r requirements-linting.txt -r requirements-coverage.txt
```

from the root directory. Strictly speaking, you wouldn't
need to install the dev dependencies, as they are installed by `tox` on the file, but they are useful for development without using tox.

This project uses the [black](https://github.com/psf/black) source code formatter
and [pre-commit](https://pre-commit.com/) to invoke it as a Git pre-commit hook.

When first cloning the repository, run the following command (after
setting up your virtualenv with dev dependencies installed) to set up
the local Git hook:

```shell script
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

## Local Development
Automated builds, tests, generation of docu and publishing are handled by cicd pipelines. 
You will find an initial version of the pipeline in this repo. Below are further details on testing 
and documentation. 

Before pushing your changes to the remote it is often useful to execute `tox` locally in order to
detect mistakes early on.


### Testing and packaging

Local testing is done with pytest and tox. Note that you can 
perform each part of the test pipeline individually as well, either
by using `tox -e <env>` or by executing the scripts in the
`build_scripts` directory directly. See the `tox.ini` file for the
list of available environments and scripts.

The library is built with tox which will build and install the package and run the test suite.
Running tox will also generate coverage and pylint reports in html and badges. 
You can configure pytest, coverage and pylint by adjusting [pytest.ini](pytest.ini), [.coveragerc](.coveragerc) and
[.pylintrc](.pylintrc) respectively.

You can run thew build by installing tox into your virtual environment 
(e.g. with `pip install tox`) and executing `tox`. 

For creating a package locally run
```shell script
python setup.py sdist bdist_wheel
```

### Documentation
Documentation is built with sphinx every time tox is executed. 
There is a helper script for updating documentation files automatically. It is called by tox on build and can 
be invoked manually as
```bash
python build_scripts/update_docs.py
```
See the code documentation in the script for more details on that.

Notebooks also form part of the documentation, although they also play the additional role of integration
tests. Have a look at the example notebook for an explanation of how this works.

### Note
You might wonder why the requirements.txt already contains numpy. The reason is that tox seems to have a problem with empty
requirements files. Feel free to remove numpy once you have non-trivial requirements


#### Automatic release process

In order to create an automatic release, a few prerequisites need to be satisfied:

- The repository needs to be on the `develop` branch
- The repository must be clean (including no untracked files)

Then, a new release can be created using the `build_scripts/release-version.sh` script (leave off the version parameter
to have `bumpversion` automatically derive the next release version):

```shell script
./build_scripts/release-version.sh 0.1.6
```

To find out how to use the script, pass the `-h` or `--help` flags:

```shell script
./build_scripts/release-version.sh --help
```

If running in interactive mode (without `-y|--yes`), the script will output a summary of pending
changes and ask for confirmation before executing the actions.

#### Manual release process
If the automatic release process doesn't cover your use case, you can also create a new release
manually by following these steps:

1. (repeat as needed) implement features on feature branches merged into `develop`. Each merge into develop will advance the `.devNNN` version suffix and publish the pre-release version into the package registry. These versions can be installed using `pip install --pre`.
2. When ready to release: Create release branch `release/vX.Y.Z` off develop and perform release activities (update changelog, news, ...). Run `bumpversion --commit release` if the release is only a patch release, otherwise the full version can be specified using `bumpversion --commit --new-version X.Y.Z release` (the `release` part is ignored but required by bumpversion :rolling_eyes:).
3. Merge the release branch into `master`, tag the merge commit, and push back to the repo. The CI pipeline publishes the package based on the tagged commit.

    ```shell script
    git checkout master
    git merge --no-ff release/vX.Y.Z
    git tag -a vX.Y.Z -m"Release vX.Y.Z"
    git push --follow-tags origin master
    ```
4. Switch back to the release branch `release/vX.Y.Z` and pre-bump the version: `bumpversion --commit patch`. This ensures that `develop` pre-releases are always strictly more recent than the last published release version from `master`.
5. Merge the release branch into `develop`:
    ```shell script
    git checkout develop
    git merge --no-ff release/vX.Y.Z
    git push origin develop
    ```
6. Delete the release branch if necessary: `git branch -d release/vX.Y.Z`
7. Pour yourself a cup of coffee, you earned it! :coffee: :sparkles:

## Useful information
Mark all autogenerated directories as excluded in your IDE. In particular docs/_build and .tox should be marked 
as excluded in order to get a significant speedup in searches and refactorings.

If using remote execution, don't forget to exclude data paths from deployment (unless you really want to sync them)
