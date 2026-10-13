from pathlib import Path
import inspect
import re
import subprocess
import typing
from enum import Enum

def format_file_with_ruff(file_path: str | Path):
    # Convert string input to a Path object safely
    path = Path(file_path)

    if not path.exists():
        print(f"Error: File '{path}' does not exist.")
        return

    try:
        # Pass the Path object directly; subprocess automatically converts it to a string
        result = subprocess.run(
            ["ruff", "format", path],
            check=True,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to format file. Exit code: {e.returncode}")
        print(f"Error output:\n{e.stderr}")
        raise e


def extract_enums_from_type(hint: typing.Any) -> set[type[Enum]]:
    """Recursively extract all Enum subclasses found within a given type hint."""
    enums = set()

    # Check if the hint itself is an Enum class
    if isinstance(hint, type) and issubclass(hint, Enum) and hint is not Enum:
        enums.add(hint)

    # Recursively look into generic arguments (e.g., list[Enum], Union[Enum, int])
    args = typing.get_args(hint)
    for arg in args:
        enums.update(extract_enums_from_type(arg))

    return enums


def clean_signature_obj(sig: inspect.Signature) -> str:
    """Removes module paths from an existing inspect.Signature object and returns its string form."""
    # Convert the signature object to a string
    sig_str = str(sig)

    # Regex pattern to remove module paths (e.g., 'typing.List' -> 'List')
    cleaned_str = re.sub(r'\b[a-zA-Z_][a-zA-Z0-9_]*\.(?=[a-zA-Z_])', '', sig_str)

    return cleaned_str
