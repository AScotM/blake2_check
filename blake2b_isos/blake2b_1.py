import hashlib
import pathlib
import io
import logging
import argparse
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_blake2b(file_path: pathlib.Path, verbose: bool = False, chunk_size: int = io.DEFAULT_BUFFER_SIZE) -> Optional[str]:
    """Calculate BLAKE2b checksum for a given file, with optional verbosity."""
    blake2 = hashlib.blake2b()
    file_size = file_path.stat().st_size
    start_time = time.time()

    logging.info(f"Processing file: {file_path.name} ({file_size} bytes)")

    try:
        with file_path.open('rb') as f:
            with tqdm(
                total=file_size,
                unit='B',
                unit_scale=True,
                disable=not verbose,
                desc=f"Hashing {file_path.name}"
            ) as pbar:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    blake2.update(chunk)
                    pbar.update(len(chunk))

        elapsed_time = time.time() - start_time
        logging.info(f" Completed: {file_path.name} | Size: {file_size} bytes | Time: {elapsed_time:.2f}s")
        return blake2.hexdigest()

    except (OSError, IOError) as e:
        logging.error(f" Error reading file {file_path}: {e}")
        return None

def process_file(file: pathlib.Path, verbose: bool, chunk_size: int) -> Optional[str]:
    blake2_hash = calculate_blake2b(file, verbose=verbose, chunk_size=chunk_size)
    if blake2_hash:
        logging.info(f"{file.name:40} BLAKE2b: {blake2_hash}")
    return blake2_hash

def check_blake2_sums(directory: str, verbose: bool = False, ext: str = '.iso', chunk_size: int = io.DEFAULT_BUFFER_SIZE):
    """Scan a directory for files with a given extension and calculate their BLAKE2b hashes."""
    dir_path = pathlib.Path(directory)

    if not dir_path.is_dir():
        logging.error(f"The specified path '{directory}' is not a valid directory.")
        return

    files = [f for f in dir_path.iterdir() if f.is_file() and f.suffix.lower() == ext]
    if not files:
        logging.warning(f"No files with extension '{ext}' found in {directory}.")
        return

    logging.info(f"Found {len(files)} file(s) with extension '{ext}' in {directory}. Starting checksum calculations...")

    success_count = 0
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, file, verbose, chunk_size): file for file in files}
        for future in as_completed(futures):
            result = future.result()
            if result:
                success_count += 1

    logging.info(f"Checksum completed. {success_count}/{len(files)} files processed successfully.")

def main():
    parser = argparse.ArgumentParser(description="Calculate BLAKE2b checksums for files in a directory.")
    parser.add_argument('directory', type=str, nargs='?', default='.',
                        help='The directory to scan for files (default: current directory)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output with progress bars')
    parser.add_argument('--ext', type=str, default='.iso', help='File extension to process (default: .iso)')
    parser.add_argument('--chunk-size', type=int, default=io.DEFAULT_BUFFER_SIZE,
                        help='Chunk size for reading files (in bytes)')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose mode enabled: Showing detailed processing logs...")

    check_blake2_sums(args.directory, verbose=args.verbose, ext=args.ext, chunk_size=args.chunk_size)

if __name__ == "__main__":
    main()
