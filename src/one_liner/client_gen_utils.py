from pathlib import Path
import subprocess

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
