#!/usr/bin/env python
import sys
import os
import hashlib
import shutil
import inspect
import gc
from datetime import datetime


def create_log_file():
    now = datetime.now()
    filename = f'./dedupe-{now.strftime("%Y%m%dT%H:%M:%S")}.log'
    log_file = open(filename, "w")
    return log_file


def write_log_message(message):
    now = datetime.now()
    print(f'{now.strftime("%Y/%m/%d %H:%M:%S")},{message}')
#    log_file.write(f'{now.strftime("%Y/%m/%d %H:%M:%S")},{message}')
    return
#    now = datetime.now()
#    standard_log_record = f'{now.strftime("%Y/%m/%d %H:%M:%S")},{message}\n'
#    file.write(standard_log_record)
#    file.flush


def chunk_reader(fobj, chunk_size=51200):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, if_true_hash_complete_file=True, hash=hashlib.sha1):
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks
    sha1 = hashlib.sha1()
    chunks = 0
    write_log_message(f'File to be hashed: {filename}')
    if if_true_hash_complete_file:
        with open(filename, 'rb') as file:
            while True:
                chunks += 1
                data = file.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
    else:
        file = open(filename, 'rb')
        chunks = 1
        data = file.read(BUF_SIZE)
        sha1.update(data)
    print("SHA1: {0}".format(sha1.hexdigest()))
    write_log_message(f' {chunks} chunks hashed')
    return sha1.digest()    


def group_files_by_size(paths):
    write_log_message(f'{inspect.currentframe().f_code.co_name} starting')
    files_by_size = {}
    minimum_file_size_bytes = 2000
    count = 0
    count_message_interval = 1000
    for path in paths:
        print(f'path in paths = {path}')
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
 #               print(f'filename in filenames: count = {count} dirpath = {dirpath} filename = {filename}')
                full_path = os.path.join(dirpath, filename)
                ''' if "/path/to/exclude" in full_path:
                    continue
                elif "/other/path/to/exclude" in full_path:
                    continue
                else:
                    pass
                '''
                try:
                    # if the target is a symlink (soft one), this will 
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    continue
                if file_size < minimum_file_size_bytes:
                    continue
                duplicate = files_by_size.get(file_size)
                if duplicate:
                    files_by_size[file_size].append(full_path)
                else:
                    files_by_size[file_size] = []  # create the list for this file size
                    files_by_size[file_size].append(full_path)
                count += 1
                if count % count_message_interval == 0:
                    write_log_message(f'{inspect.currentframe().f_code.co_name} count = {str(count)}')
    write_log_message(f'records processed = {str(count)}')
    write_log_message(f'{inspect.currentframe().f_code.co_name} complete')
    return files_by_size


def regroup_files_adding_hash(files_by_group,if_true_hash_complete_file):
    write_log_message(f'{inspect.currentframe().f_code.co_name} starting')
    regrouped_files = {}
    min_group_size = 2 # we only want to get hashes where we have potential duplicates
    for original_group, files in files_by_group.items():
        if len(files) >= min_group_size:
            for filename in files:
                file_hash = get_hash(filename, if_true_hash_complete_file)
                new_group = f'{original_group}-{file_hash.hex()}'
                duplicate = regrouped_files.get(new_group)
                if duplicate:
                    regrouped_files[new_group].append(filename)
                else:
                    regrouped_files[new_group] = []          # create the list for this 1k hash
                    regrouped_files[new_group].append(filename)
    write_log_message(f'{inspect.currentframe().f_code.co_name} complete')
    return regrouped_files


def process_duplicate_files(duplicate_files):
    write_log_message(f'-- Start of duplicate file list --')
    for file_hash, files in duplicate_files.items():
            for filepaths in files:
                head_tail = os.path.split(filepaths) 
                path = head_tail[0]
                file = head_tail[1]
                write_log_message(f'{file_hash},{file},{path}')
    write_log_message(f'-- End of duplicate file list --')


def check_for_duplicates(paths, hash=hashlib.sha1):
    # Group all files based on size
    files_grouped_by_size = group_files_by_size(paths)
    # Regroup files, adding the hash for the first chunk of the file to their index
    files_grouped_by_size_and_first_chunk_hash = regroup_files_adding_hash(files_grouped_by_size,False)
    # Regroup files, adding the hash for the whole file to their index
    files_grouped_by_size_and_hash = regroup_files_adding_hash(files_grouped_by_size_and_first_chunk_hash,True)
    # Identify the files that have the same hash as at least one other file - these are duplicates   
    duplicate_files = {}
    for file_hash, files in files_grouped_by_size_and_hash.items():
        if len(files) > 1:
            for filename in files:
                duplicate = duplicate_files.get(file_hash)
                if duplicate:
                    duplicate_files[file_hash].append(filename)
                else:
                    duplicate_files[file_hash] = []
                    duplicate_files[file_hash].append(filename)
    write_log_message(f'duplicate files identified')
    return(duplicate_files)  


def main():
    global log_file
    log_file = create_log_file()
    write_log_message(f'script starting')
    duplicate_files = check_for_duplicates(sys.argv[1:])
    process_duplicate_files(duplicate_files) 
    write_log_message(f'script completed')


if __name__ == '__main__':
    if sys.argv[1:]:
        main()
        sys.exit()
    else:
        print('Please pass the paths to check as parameters to the script')

