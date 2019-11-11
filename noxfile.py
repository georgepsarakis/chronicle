import nox

nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = True
nox.options.keywords = "not serve"

source_files = ("chronicle", "tests", "setup.py", "noxfile.py")


def install_dependencies(session):
    session.install(
        "--upgrade",
        "-r", "install/requirements/dev.txt",
        "-r", "install/requirements/base.txt"
    )
    # Install project
    session.install('-e', '.')


@nox.session
def lint(session):
    install_dependencies(session)

    session.run("flake8", *source_files)
    session.run("black", "--target-version=py36", *source_files)

    check(session)


@nox.session
def check(session):
    install_dependencies(session)

    session.run(
        "black",
        "--check",
        "--diff",
        "--target-version=py36",
        *source_files,
        success_codes=[0, 1]
    )
    session.run("flake8", *source_files)


@nox.session(python=['3', "3.6", "3.7", "3.8"])
def test(session):
    install_dependencies(session)
    session.run("python", "-m", "pytest", "-v", "tests")
