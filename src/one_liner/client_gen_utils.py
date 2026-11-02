from pathlib import Path
import ast
import getpass
import inspect
import logging
import re
import socket
import subprocess
import types
import typing
from datetime import datetime
from enum import Enum
from jinja2 import Environment, FileSystemLoader
from typing import get_type_hints
from one_liner.client import RouterClient
from one_liner.server import RouterServer

logger = logging.getLogger(__name__)

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


def hint_is_builtin_or_enum(hint) -> bool:
    # Handle None or NoneType explicitly
    if hint is None or hint is type(None):
        return True

    # Handle typing.Any
    if hint is typing.Any:
        return True

    origin = typing.get_origin(hint)
    args = typing.get_args(hint)

    if origin is not None:
        # Define accepted union types (typing.Union and Python 3.10+ types.UnionType)
        union_types = {typing.Union}
        if hasattr(types, "UnionType"):
            union_types.add(types.UnionType)

        # If it's a union, every option inside the union must be valid
        if origin in union_types:
            return all(hint_is_builtin_or_enum(arg) for arg in args)

        # Special handling for Literal values (e.g., Literal["a", 1])
        if origin is typing.Literal:
            return all(isinstance(arg, (int, str, bool, bytes, Enum)) for arg in args)

        # For standard generic containers (e.g., list[int], dict[str, MyEnum])
        return hint_is_builtin_or_enum(origin) and all(hint_is_builtin_or_enum(arg) for arg in args)

    # If it's not a class/type, return False
    if not isinstance(hint, type):
        return False

    # Check if it's a Python built-in type (int, str, list, etc.)
    if hint.__module__ == "builtins":
        return True

    # Check if it's derived from an Enum
    try:
        if issubclass(hint, Enum):
            return True
    except TypeError:
        pass

    return False


def clean_signature_obj(sig: inspect.Signature) -> str:
    """Removes module paths from an existing inspect.Signature object and returns its string form."""
    # Convert the signature object to a string
    sig_str = str(sig)

    # Regex pattern to remove module paths (e.g., 'typing.List' -> 'List')
    cleaned_str = re.sub(r'\b[a-zA-Z_][a-zA-Z0-9_]*\.(?=[a-zA-Z_])', '', sig_str)

    return cleaned_str


def sanitize_stream_name(s: str) -> str:
    """Strip special characters from stream name and remove preceding underscores"""
    # Replace whitespace and common punctuation with underscores
    sanitized = re.sub(r"[\s\-\+\=\*\/\%\!\@\#\$\^\&\(\)\[\]\{\}\:\;\,\.\<\>\?\|\\`~]", "_", s)
    # Collapse consecutive underscores into a single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    sanitized = sanitized.strip("_")
    return sanitized


def create_interface_class(
    server: RouterServer,
    class_name: str,
    output_dir: Path | str | None = None):
    """Generate a python client class for interfacing with all named
    calls and streams."""
    # Validate class name:
    try:
        ast.parse(f"class {class_name}: pass")
    except SyntaxError:
        raise SyntaxError(f"Class name {class_name} is invalid.")

    # Sanitize output path.
    output_dir = Path() if output_dir is None else Path(output_dir)

    # Aggregate nested Enums and check for non-primitive non-enum types.
    enums = set()
    for named_call, (obj, attr, pre_args, pre_kwargs) in server.rpc.named_call_signatures.items():
        func = getattr(server.instances[obj], attr)
        hints = list(get_type_hints(func).values())
        for hint in hints:
            # Skip aggregating enums on functions that we can't support
            if not hint_is_builtin_or_enum(hint):
                continue
                #raise SyntaxError("Cannot generate client class interface for "
                #    f"non-builtin and non-enum type: {hint}!")
            enums |= extract_enums_from_type(hint)

    # Aggregate stream return types and check for non-primitive and non-enum types.
    streams = {}
    for stream_name, stream_sig in server.streamer._call_signature.items():
        stream_func, _, _ = stream_sig
        sig = inspect.signature(stream_func)
        return_annotation = (sig.return_annotation
            if sig.return_annotation is not inspect.Signature.empty else None)
        if not hint_is_builtin_or_enum(return_annotation):
            msg = (f"Cannot create function that uses parameter of complex "
                f"type {return_annotation} for stream: {stream_name}")
            logger.error(msg)
            continue

        streams[stream_name] = {
            "formatted_return_hint": inspect.formatannotation(return_annotation),
            "func_friendly_name": sanitize_stream_name(stream_name)
        }

    # Create functions for all named calls. Use function signature.
    named_call_signatures = {}
    for named_call, (obj, attr, pre_args, pre_kwargs) in server.rpc.named_call_signatures.items():
        sig = inspect.signature(getattr(server.instances[obj], attr))
        # Remove any pre-filled args and kwargs.
        bound_params = sig.bind_partial(*pre_args, **pre_kwargs)
        bound_names = set(bound_params.arguments.keys())
        # Filter out parameters that were already supplied
        new_params = [
            param for name, param in sig.parameters.items()
            if name not in bound_names
        ]
        # Skip functions that have complex parameter types.
        for name, param in sig.parameters.items():
            if (not hint_is_builtin_or_enum(param.annotation)
                and param.annotation is not inspect.Signature.empty):
                msg = (f"Cannot create function that uses parameter of complex "
                    f"type {param} for named_call: {named_call}")
                logger.error(msg)
                continue
        # Skip functions that have complex return types.
        return_annotation = (sig.return_annotation
            if sig.return_annotation is not inspect.Signature.empty else None)
        if not hint_is_builtin_or_enum(return_annotation):
            msg = (f"Cannot create function that uses parameter of complex "
                f"type {return_annotation} for named_call: {named_call}")
            logger.error(msg)
            continue
        # Patch in "self" separately such that any prefilled args don't remove it.
        self_param = inspect.Parameter(
            'self',
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        new_params  = [self_param] + new_params

        # Create list of arg kwarg names
        arg_names = []
        # MUST be passed as keyword arguments (e.g., func(key=value))
        kwarg_names = []
        for name, param in sig.parameters.items():
            if name in bound_names:  # skip pre-filled args/kwargs
                continue
            if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD):
                arg_names.append(name)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                kwarg_names.append(name)

        # Return a new Signature without those parameters
        named_call_signatures[named_call] = {
            "signature": clean_signature_obj(sig.replace(parameters=new_params)),
            "args": arg_names,
            "kwargs": kwarg_names
        }

    # Create getter functions for all streams.
    # Create jinja2 template metadata.
    context = {
        "generation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user": getpass.getuser(),
        "source_pc": socket.gethostname(),
        "router_client_signature": inspect.signature(RouterClient.__init__),
        "class_name": f"{class_name}",
        "named_calls": named_call_signatures,
        "streams": streams,
        "enum_sources": [inspect.getsource(e) for e in enums]
    }

    template_dir = Path(__file__).parent.resolve() / "templates"
    env = Environment(
        loader=FileSystemLoader(template_dir))
    template = env.get_template('client_class.py.jinja')
    rendered_code = template.render(context)

    output_filename = Path(f"{class_name.lower()}_client.py")
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(rendered_code)

    format_file_with_ruff(output_dir / output_filename)
