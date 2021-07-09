import nox

@nox.session(python=["3.7", "3.8", "3.9"])
def dev(session):
    session.install("-r", "requirements.txt")
    session.install("pytest", "autopep8")
