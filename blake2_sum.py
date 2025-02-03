import hashlib
import pathlib
import io

def calculate_blake2b(file_path):
    """Calculate BLAKE2b checksum for a given file."""
    blake2 = hashlib.blake2b()
    try:
        with file_path.open('rb') as f:
            for chunk in iter(lambda: f.read(io.DEFAULT_BUFFER_SIZE), b''):
                blake2.update(chunk)
        return blake2.hexdigest()
    except (OSError, IOError) as e:
        return f"Error: {e}"

def check_blake2_sums(directory):
    """Scan a directory for `.iso` files and calculate their BLAKE2 hashes."""
    dir_path = pathlib.Path(directory)

    for file in dir_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.iso':
            blake2_hash = calculate_blake2b(file)
            print(f"{file.name:40} BLAKE2b: {blake2_hash}")

# Run function in current directory
if __name__ == "__main__":
    check_blake2_sums(pathlib.Path("."))

