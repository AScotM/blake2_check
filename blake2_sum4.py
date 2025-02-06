import hashlib
import pathlib
import io
import logging
import argparse
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_blake2b(file_path, verbose=False, chunk_size=io.DEFAULT_BUFFER_SIZE):
    """Calculate BLAKE2b checksum for a given file, with optional verbosity."""
    blake2 = hashlib.blake2b()
    file_size = file_path.stat().st_size
    start_time = time.time()

    logging.info(f"Processing file: {file_path.name} ({file_size} bytes)")

    try:
        with file_path.open('rb') as f:
            for chunk in tqdm(iter(lambda: f.read(chunk_size), b''), total=(file_size // chunk_size) + 1, disable=not verbose):
                blake2.update(chunk)

        elapsed_time = time.time() - start_time
        logging.info(f"✅ Completed: {file_path.name} | Size: {file_size} bytes | Time: {elapsed_time:.2f}s")
        return blake2.hexdigest()

    except (OSError, IOError) as e:
        logging.error(f"❌ Error reading file {file_path}: {e}")
        return None

def check_blake2_sums(directory, verbose=False, ext='.iso', chunk_size=io.DEFAULT_BUFFER_SIZE):
    """Scan a directory for files with a given extension and calculate their BLAKE2b hashes."""
    dir_path = pathlib.Path(directory)

    if not dir_path.is_dir():
        logging.error(f"❌ The specified path '{directory}' is not a valid directory.")
        return

    with ThreadPoolExecutor() as executor:
        for file in dir_path.iterdir():
            if file.is_file() and file.suffix.lower() == ext:
                executor.submit(process_file, file, verbose, chunk_size)

def process_file(file, verbose, chunk_size):
    blake2_hash = calculate_blake2b(file, verbose=verbose, chunk_size=chunk_size)
    if blake2_hash:
        logging.info(f"{file.name:40} BLAKE2b: {blake2_hash}")

def main():
    parser = argparse.ArgumentParser(description="Calculate BLAKE2b checksums for files in a directory.")
    parser.add_argument('directory', type=str, nargs='?', default='.', 
                        help='The directory to scan for files (default: current directory)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--ext', type=str, default='.iso', help='File extension to process (default: .iso)')
    parser.add_argument('--chunk-size', type=int, default=io.DEFAULT_BUFFER_SIZE, 
                        help='Chunk size for reading files (in bytes)')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        print("🔹 Verbose mode enabled: Showing detailed processing logs...")

    check_blake2_sums(args.directory, verbose=args.verbose, ext=args.ext, chunk_size=args.chunk_size)

if __name__ == "__main__":
    main()
