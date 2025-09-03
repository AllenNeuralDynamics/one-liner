# aind-python-library-template
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)


This is a repository template to quickly setup a python library project. This repository utilizes a tool called **uv** to handle all dependency and package management. For more information on this tool go to the [uv wiki](https://docs.astral.sh/uv/). 

##  Getting Started

- To use this template, click the green ``Use this template`` button and ``Create new repository``.
- [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

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
