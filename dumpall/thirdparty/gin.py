#!/usr/bin/env python3
"""
gin - a Git index file parser with enhanced functionality
"""

import binascii
import collections
import json
import mmap
import struct
import argparse
import os
import sys
from typing import Generator, OrderedDict


# Global version
VERSION = "0.2.001"


class ParsingError(Exception):
    """Custom exception for errors encountered during parsing."""
    pass


class GitIndexParser:
    """
    Main class for parsing Git index files with enhanced features.
    """

    def __init__(self, filename: str, pretty: bool = True):
        self.filename = filename
        self.pretty = pretty
        self.logger = Logger(enable_debug=True)

    def parse(self) -> Generator[OrderedDict, None, None]:
        """Main parsing logic for Git index files."""
        with open(self.filename, "rb") as o:
            f = mmap.mmap(o.fileno(), 0, access=mmap.ACCESS_READ)

            def read(format: str):
                """Read bytes using the given struct format."""
                format = "!" + format
                bytes = f.read(struct.calcsize(format))
                return struct.unpack(format, bytes)[0]

            # Parse header
            index = collections.OrderedDict()
            index["signature"] = f.read(4).decode("ascii")
            self.logger.check(index["signature"] == "DIRC", "Not a Git index file.")

            index["version"] = read("I")
            self.logger.check(
                index["version"] in {2, 3},
                f"Unsupported version: {index['version']}",
            )

            index["entries"] = read("I")
            self.logger.info(f"Parsed header: {index}")
            yield index

            # Parse entries
            for n in range(index["entries"]):
                entry = self._parse_entry(f, read, n + 1)
                yield entry

            # Parse extensions
            while f.tell() < (len(f) - 20):
                extension = self._parse_extension(f, read)
                yield extension

            # Parse checksum
            checksum = self._parse_checksum(f)
            yield checksum

    def _parse_entry(self, f, read, entry_number):
        """Parses a single entry."""
        entry = collections.OrderedDict()
        entry["entry"] = entry_number
        entry["ctime_seconds"] = read("I")
        entry["ctime_nanoseconds"] = read("I")
        entry["mtime_seconds"] = read("I")
        entry["mtime_nanoseconds"] = read("I")
        entry["dev"] = read("I")
        entry["ino"] = read("I")
        entry["mode"] = f"{read('I'):06o}"
        entry["uid"] = read("I")
        entry["gid"] = read("I")
        entry["size"] = read("I")
        entry["sha1"] = binascii.hexlify(f.read(20)).decode("ascii")
        entry["flags"] = read("H")
        entry["name"] = self._read_name(f, entry["flags"])
        self.logger.debug(f"Parsed entry: {entry}")
        return entry

    def _read_name(self, f, flags):
        """Reads the name of the entry."""
        namelen = flags & 0xFFF
        if namelen < 0xFFF:
            return f.read(namelen).decode("utf-8", "replace")
        name = []
        while (byte := f.read(1)) != b"\x00":
            name.append(byte)
        return b"".join(name).decode("utf-8", "replace")

    def _parse_extension(self, f, read):
        """Parses an extension block."""
        extension = collections.OrderedDict()
        extension["signature"] = f.read(4).decode("ascii")
        extension["size"] = read("I")
        extension["data"] = f.read(extension["size"]).decode("utf-8", "replace")
        self.logger.info(f"Parsed extension: {extension}")
        return extension

    def _parse_checksum(self, f):
        """Parses the checksum."""
        checksum = collections.OrderedDict()
        checksum["checksum"] = True
        checksum["sha1"] = binascii.hexlify(f.read(20)).decode("ascii")
        self.logger.info(f"Parsed checksum: {checksum}")
        return checksum


class Logger:
    """Simple logging utility."""

    def __init__(self, enable_debug=False):
        self.enable_debug = enable_debug

    def debug(self, message):
        if self.enable_debug:
            print(f"[DEBUG] {message}")

    def info(self, message):
        print(f"[INFO] {message}")

    def check(self, condition, message):
        if not condition:
            print(f"[ERROR] {message}", file=sys.stderr)
            sys.exit(1)


def parse_file(arg, pretty=True):
    """Parses a Git index file and outputs the results."""
    parser = GitIndexParser(arg, pretty)
    for item in parser.parse():
        if pretty:
            print(json.dumps(item, indent=2))
        else:
            print(json.dumps(item))


def main():
    """Main function for CLI usage."""
    parser = argparse.ArgumentParser(description="Parse a Git index file.")
    parser.add_argument("-j", "--json", action="store_true", help="Output JSON.")
    parser.add_argument(
        "-v", "--version", action="store_true", help="Show script version."
    )
    parser.add_argument(
        "path", nargs="?", default=".", help="Path to a Git repository or index file."
    )
    args = parser.parse_args()

    if args.version:
        print(f"gin {VERSION}")
        sys.exit()

    path = args.path
    if os.path.isdir(path):
        path = os.path.join(path, ".git", "index")
        if not os.path.isfile(path):
            print("Error: Could not find .git/index file.", file=sys.stderr)
            sys.exit(1)

    parse_file(path, pretty=not args.json)


if __name__ == "__main__":
    main()

