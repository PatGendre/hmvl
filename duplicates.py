#!/usr/bin/env python
"""
Fast duplicate file finder.
Usage: duplicates.py <folder> [<folder>...]python duplicates.py ../rdc_0/2020-04-25 ../rdc_1/2020-04-25
Based on https://stackoverflow.com/a/36113168/300783
Modified for Python3 with some small code improvements.
adapté aux fichiers hmvl DIRMED 24/04/2020

exemple:
python duplicates.py ../rdc_0/2020-04-25 ../rdc_1/2020-04-25
donne comme résultat 
   (find  ../rdc_*/2020-04-02 -name "*00" -type f|wc -l)
8000 fichiers RD (se terminent pas 00), 112 fichiers csv (Labocom), 19000 fichiers en doublons
"""
import os
import sys
import hashlib
from collections import defaultdict


def chunk_reader(fobj, chunk_size=1024):
    """ Generator that reads a file in chunks of bytes """
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash_algo=hashlib.sha1):
    hashobj = hash_algo()
    with open(filename, "rb") as f:
        if first_chunk_only:
            hashobj.update(f.read(1024))
        else:
            for chunk in chunk_reader(f):
                hashobj.update(chunk)
    return hashobj.digest()


def check_for_duplicates(paths):
    files_by_size = defaultdict(list)
    files_by_small_hash = defaultdict(list)
    files_by_full_hash = dict()
    print("Liste des fichiers par taille")
    nfichiers=0
    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                nfichiers+=1
                full_path = os.path.join(dirpath, filename)
                try:
                    # if the target is a symlink (soft one), this will
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except OSError:
                    # not accessible (permissions, etc) - pass on
                    continue
                files_by_size[file_size].append(full_path)
    print(str(nfichiers)+" fichiers pour "+str(len(files_by_size))+ " tailles de fichiers")
    # For all files with the same file size, get their hash on the first 1024 bytes
    nuniques=0
    ndoublons=0
    for files in files_by_size.values():
        if len(files) < 2:
            nuniques+=1
            continue  # this file size is unique, no need to spend cpu cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
            except OSError:
                # the file access might've changed till the exec point got here
                continue
            files_by_small_hash[small_hash].append(filename)
    print (str(nuniques)+ " fichiers de taille unique")
    # For all files with the hash on the first 1024 bytes, get their hash on the full
    # file - collisions will be duplicates
    for files in files_by_small_hash.values():
        if len(files) < 2:
            # the hash of the first 1k bytes is unique -> skip this file
            continue

        for filename in files:
            try:
                full_hash = get_hash(filename, first_chunk_only=False)
            except OSError:
                # the file access might've changed till the exec point got here
                continue

            if full_hash in files_by_full_hash:
                duplicate = files_by_full_hash[full_hash]
                if os.path.basename(filename)==os.path.basename(duplicate):
                    os.rename(filename,filename+".dup")
                else:
                    os.rename(filename,filename+".dd")
                ndoublons+=1
                print("Renommage d'un doublon : %s de %s" % (filename, duplicate))
            else:
                files_by_full_hash[full_hash] = filename


if __name__ == "__main__":
    if sys.argv[1:]:
        check_for_duplicates(sys.argv[1:])
    else:
        print("Usage: %s <folder> [<folder>...]" % sys.argv[0])
