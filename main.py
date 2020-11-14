import os
import hashlib
import argparse
import json

CHUNK_SIZE = 1024 * 1024
ITER_NORMAL = 8
ITER_NITRO = 50


def cleanup(files):
    """ delete chunk and header files """
    for e in files:
        try:
            os.remove(e)
        except OSError as exception:
            print(f"Couldn't delete file: {exception}")
            return


def split(input_path, output_directory=None, nitro=False):
    if not output_directory:
        output_directory = os.getcwd()
    md5_hash = hashlib.md5()
    file_name = os.path.basename(input_path)
    file_size = os.path.getsize(input_path)
    if nitro:
        max_iter = 50
    else:
        max_iter = 8
    amount_chunks = int(file_size / (CHUNK_SIZE * max_iter)) + 1
    file_counter = 1
    with open(input_path, "rb") as f:
        for _ in range(amount_chunks):
            chunk = open(f"{output_directory}/{file_name}_{file_counter}-{amount_chunks}.chunk", "ab")
            for _ in range(max_iter):
                if data := f.read(CHUNK_SIZE):
                    chunk.write(data)
                    md5_hash.update(data)
                else:
                    break
            chunk.close()
            file_counter += 1
    header_data = {"name": file_name, "md5": md5_hash.hexdigest(), "length": amount_chunks}
    with open(f"{output_directory}/header.json", "w+") as header:
        json.dump(header_data, header)


def concatenate(input_directory, output_directory=None):
    if not output_directory:
        output_directory = os.getcwd()
    md5_hash = hashlib.md5()
    with open(f"{input_directory}/header.json", "r") as header:
        header_data = json.load(header)
    original_file_name = header_data["name"]
    original_md5_hash = header_data["md5"]
    original_length = header_data["length"]
    all_files = [f"{input_directory}/header.json"]
    for i in range(1, original_length + 1):
        all_files.append(f"{input_directory}/{original_file_name}_{i}-{original_length}.chunk")
    with open(f"{output_directory}/{original_file_name}", "ab") as output:
        # without header file
        for e in all_files[1:]:
            with open(e, "rb") as chunk:
                file_content = chunk.read()
                output.write(file_content)
                md5_hash.update(file_content)
    final_md5_hash = md5_hash.hexdigest()
    if final_md5_hash == original_md5_hash:
        cleanup(all_files)
    else:
        try:
            os.remove(f"{output_directory}/{original_file_name}")
        except OSError:
            print("Couldn't delete invalid output file.")
        print(f"Error while checking MD5 Hashes.\nOriginal Hash: {original_md5_hash}\nNew Hash: {final_md5_hash}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", "-s", metavar="PATH", help="split file at specific path")
    parser.add_argument("--concatenate", "-c", metavar="DIRECTORY", help="directory containing header and chunk files")
    parser.add_argument("--nitro", "-n", action="store_true", help="Increase chunk size from 8MB to 50MB.")
    parser.add_argument("--output", "-o", metavar="DIRECTORY", help="Choose output directory.")
    args = parser.parse_args()
    if args.split:
        split(args.split, args.output, args.nitro)
    elif args.concatenate:
        concatenate(args.concatenate, args.output)
    else:
        parser.error("Either --split/-s or --concatenate/-c is required")
