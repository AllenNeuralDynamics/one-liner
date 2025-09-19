# one-liner
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)

<img src="./assets/train-conductor-mouse.png" width="180">


A ZMQ-based Router pattern for interacting with remote python objects.

The router pattern provides a bridge between decoupled applications from interfaces (i.e: the model from the view).
This separation provides several advantages:
* GUIs can be developed independently of standalone projects.
* GUIs can run on separate processes or on different machines altogether, providing more flexibility where some machines are resource-constrained.
* Failures are siloed. A GUI can crash independent of the application it is interfacing with.

## Package Installation


to install this package, in the root directory, run
```bash
pip install -e .
```

To install this package with supplementary dependencies for writing documentation, in the root directory, run:
```bash
pip install -e .[dev]
```

To install *and develop this package in its own environment*, in the root directory, run:
```bash
uv sync
uv pip install -e .  # or .[dev]
```

## Tools

### Package/Project Management

This project utilizes [uv](https://docs.astral.sh/uv/) to handle installing dependencies as well as setting up environments for this project. It replaces tool like pip, poetry, virtualenv, and conda.

This project also uses [tox](https://tox.wiki/en/latest/index.html) for orchestrating multiple testing environments that mimics the github actions CI/CD so that you can test the workflows locally on your machine before pushing changes.

### Code Quality Check

The following are tools used to ensure code quality in this project.

- Unit Testing

```bash
uv run pytest tests
```

- Linting

```bash
uv run ruff check
```

- Type Check

```bash
uv run mypy src/mypackage
```

## Documentation
To generate the rst files source files for documentation, run
```bash
sphinx-apidoc -o docs/source/ src
```
Then to create the documentation HTML files, run
```bash
sphinx-build -b html docs/source/ docs/build/html
```
