import hashlib
import pathlib
import io
import logging
import argparse
import time

# Default logging setup (INFO level)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_blake2b(file_path, verbose=False):
    """Calculate BLAKE2b checksum for a given file, with optional verbosity."""
    blake2 = hashlib.blake2b()
    file_size = file_path.stat().st_size  # Get file size
    start_time = time.time()  # Start timing

    logging.info(f"Processing file: {file_path.name} ({file_size} bytes)")

    try:
        with file_path.open('rb') as f:
            chunk_count = 0
            while chunk := f.read(io.DEFAULT_BUFFER_SIZE):
                blake2.update(chunk)
                chunk_count += 1
                if verbose:
                    logging.debug(f"Processed chunk {chunk_count}: {len(chunk)} bytes")

        elapsed_time = time.time() - start_time
        logging.info(f"✅ Completed: {file_path.name} | Size: {file_size} bytes | Time: {elapsed_time:.2f}s")
        return blake2.hexdigest()

    except (OSError, IOError) as e:
        logging.error(f"❌ Error reading file {file_path}: {e}")
        return None

def check_blake2_sums(directory, verbose=False):
    """Scan a directory for `.iso` files and calculate their BLAKE2b hashes."""
    dir_path = pathlib.Path(directory)

    if not dir_path.is_dir():
        logging.error(f"❌ The specified path '{directory}' is not a valid directory.")
        return

    for file in dir_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.iso':
            blake2_hash = calculate_blake2b(file, verbose=verbose)
            if blake2_hash:
                logging.info(f"{file.name:40} BLAKE2b: {blake2_hash}")

def main():
    parser = argparse.ArgumentParser(description="Calculate BLAKE2b checksums for .iso files in a directory.")
    parser.add_argument('directory', type=str, nargs='?', default='.', 
                        help='The directory to scan for .iso files (default: current directory)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    # Adjust logging level if verbose is enabled
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        print("🔹 Verbose mode enabled: Showing detailed processing logs...")

    check_blake2_sums(args.directory, verbose=args.verbose)

if __name__ == "__main__":
    main()

