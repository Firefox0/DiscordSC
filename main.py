import os
import hashlib
import argparse
import json

CHUNK_SIZE = 4096
MAX_ITER = 2000


def cleanup(directory):
    """ delete chunk and header files """
    try:
        os.remove(f"{directory}/header.json")
    except OSError:
        print("Header file does not exist or can't be deleted.")
        return
    for e in os.listdir(directory):
        if ".chunk" in e:
            try:
                os.remove(e)
            except OSError:
                print("Couldn't delete chunk file.")
                return


def split(input_path, output_directory=None):
    if not output_directory:
        output_directory = os.getcwd()
    md5_hash = hashlib.md5()
    file_name = os.path.basename(input_path)
    file_size = os.path.getsize(input_path)
    amount_chunks = int(file_size / (CHUNK_SIZE * MAX_ITER)) + 1
    file_counter = 1
    with open(input_path, "rb") as f:
        for _ in range(amount_chunks):
            chunk = open(f"{output_directory}/{file_name}_{file_counter}-{amount_chunks}.chunk", "ab")
            for _ in range(MAX_ITER):
                if data := f.read(CHUNK_SIZE):
                    chunk.write(data)
                    md5_hash.update(data)
                else:
                    break
            chunk.close()
            file_counter += 1
    header_data = {"name": file_name, "md5": md5_hash.hexdigest()}
    with open(f"{output_directory}/header.json", "w+") as header:
        json.dump(header_data, header)


def concatenate(input_directory, output_directory=None):
    if not output_directory:
        output_directory = os.getcwd()
    md5_hash = hashlib.md5()
    with open(f"{output_directory}/header.json", "r") as header:
        header_data = json.load(header)
    original_file_name = header_data["name"]
    original_md5_hash = header_data["md5"]
    final_file = open(f"{output_directory}/{original_file_name}", "ab")
    for f in os.listdir(input_directory):
        if ".chunk" in f:
            with open(f, "rb") as chunk:
                file_content = chunk.read()
                final_file.write(file_content)
                md5_hash.update(file_content)
    final_file.close()
    final_md5_hash = md5_hash.hexdigest()
    if final_md5_hash == original_md5_hash:
        cleanup(output_directory)
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
    args = parser.parse_args()
    if args.split:
        split(args.split)
    elif args.concatenate:
        concatenate(args.concatenate)
    else:
        parser.error("Either --split/-s or --concatenate/-c is required")
