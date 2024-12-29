import struct
import logging


class ParsingError(Exception):
    """Custom exception for errors encountered during parsing."""
    pass


class Logger:
    """
    Simple logging utility for debugging.
    """
    def __init__(self, enable_debug=False):
        self.enable_debug = enable_debug

    def debug(self, message):
        if self.enable_debug:
            print(f"[DEBUG] {message}")

    def info(self, message):
        print(f"[INFO] {message}")

    def error(self, message):
        print(f"[ERROR] {message}")


class DataBlock:
    """
    Class representing a block of data in a .DS_Store file.
    Provides methods for reading and manipulating the data within the block.
    """

    def __init__(self, data, debug=False):
        """
        Initialize the DataBlock with raw binary data.

        Args:
            data (bytes): Raw binary data of the block.
            debug (bool): Enable or disable debugging.
        """
        self.data = data
        self.pos = 0
        self.logger = Logger(debug)

    def offset_read(self, length, offset=None):
        """
        Reads a specific number of bytes from the data at a given offset.

        Args:
            length (int): Number of bytes to read.
            offset (int, optional): Starting offset to read from. Defaults to None.

        Returns:
            bytes: The read data.

        Raises:
            ParsingError: If the requested length exceeds the data size.
        """
        offset_position = self.pos if offset is None else offset

        if len(self.data) < offset_position + length:
            raise ParsingError("Requested length exceeds available data size.")

        if offset is None:
            self.pos += length

        value = self.data[offset_position:offset_position + length]
        self.logger.debug(f"Reading bytes {offset_position}-{offset_position + length}: {value}")
        return value

    def skip(self, length):
        """
        Skips a given number of bytes in the data.

        Args:
            length (int): Number of bytes to skip.
        """
        self.pos += length
        self.logger.debug(f"Skipped {length} bytes, new position: {self.pos}")

    def read_string(self, length):
        """
        Reads a UTF-16 encoded string from the data.

        Args:
            length (int): Length of the string in characters.

        Returns:
            str: The decoded string.
        """
        raw_data = self.offset_read(length * 2)
        decoded_string = raw_data.decode("utf-16be")
        self.logger.debug(f"Read string: {decoded_string}")
        return decoded_string

    def read_structure_type(self):
        """
        Reads the structure type (4-character code) from the current position.

        Returns:
            str: The structure type.
        """
        structure_type = self.offset_read(4).decode()
        self.logger.debug(f"Structure type: {structure_type}")
        return structure_type

    def read_filename(self):
        """
        Reads and parses a filename block.

        Returns:
            str: The extracted filename.
        """
        length = struct.unpack(">I", self.offset_read(4))[0]
        filename = self.read_string(length)
        structure_id = struct.unpack(">I", self.offset_read(4))[0]
        structure_type = self.read_structure_type()

        self.logger.debug(f"Filename: {filename}, Structure ID: {structure_id}, Type: {structure_type}")
        self.skip(self._calculate_skip_length(structure_type))

        return filename

    def _calculate_skip_length(self, structure_type):
        """
        Calculates the number of bytes to skip based on the structure type.

        Args:
            structure_type (str): The structure type.

        Returns:
            int: Number of bytes to skip.
        """
        skip_mapping = {
            "bool": 1,
            "long": 4,
            "blob": lambda: struct.unpack(">I", self.offset_read(4))[0],
        }

        if structure_type in skip_mapping:
            skip = skip_mapping[structure_type]
            return skip() if callable(skip) else skip

        self.logger.debug(f"Unknown structure type: {structure_type}. Defaulting to 0 skip.")
        return 0


class DS_Store:
    """
    Main parser for .DS_Store files.
    Handles the parsing of headers, offsets, table of contents (ToC), and file metadata.
    """

    def __init__(self, data, debug=False):
        """
        Initializes the DS_Store parser.

        Args:
            data (bytes): Raw binary data of the .DS_Store file.
            debug (bool): Enable or disable debugging.
        """
        self.data = data
        self.logger = Logger(debug)
        self.block = DataBlock(data, debug=debug)

        self.header = self._read_header()
        self.offsets = self._read_offsets()
        self.toc = self._read_toc()

    def _read_header(self):
        """
        Reads and validates the header of the .DS_Store file.

        Returns:
            DataBlock: Parsed header data.
        """
        if len(self.data) < 36:
            raise ParsingError("Data length insufficient for header parsing.")

        magic1, magic2 = struct.unpack(">II", self.block.offset_read(8))
        if magic1 != 0x1 or magic2 != 0x42756431:
            raise ParsingError("Invalid magic bytes in header.")

        offset, size, offset2 = struct.unpack(">III", self.block.offset_read(12))
        if offset != offset2:
            raise ParsingError("Header offset mismatch.")

        self.block.skip(16)
        self.logger.info("Header parsed successfully.")
        return DataBlock(self.block.offset_read(size, offset + 4), debug=self.logger.enable_debug)

    def _read_offsets(self):
        """
        Reads the offset table from the file.

        Returns:
            list: List of offsets.
        """
        count = struct.unpack(">I", self.block.offset_read(4))[0]
        self.block.skip(4)  # Always zero

        offsets = [struct.unpack(">I", self.block.offset_read(4))[0] for _ in range(count)]
        self.logger.info(f"Offsets read: {offsets}")
        return offsets

    def _read_toc(self):
        """
        Reads the table of contents (ToC).

        Returns:
            dict: ToC entries mapping names to block IDs.
        """
        count = struct.unpack(">I", self.block.offset_read(4))[0]
        toc = {}

        for _ in range(count):
            toc_len = struct.unpack(">B", self.block.offset_read(1))[0]
            toc_name = self.block.offset_read(toc_len).decode()
            block_id = struct.unpack(">I", self.block.offset_read(4))[0]
            toc[toc_name] = block_id

        self.logger.info(f"ToC read: {toc}")
        return toc

    def traverse(self, block_id):
        """
        Traverses a block and extracts filenames.

        Args:
            block_id (int): ID of the block to traverse.

        Returns:
            list: List of filenames.
        """
        node = self._block_by_id(block_id)
        next_pointer, count = struct.unpack(">II", node.offset_read(8))

        filenames = [node.read_filename() for _ in range(count)]

        if next_pointer > 0:
            filenames += self.traverse(next_pointer)

        return filenames

    def _block_by_id(self, block_id):
        """
        Retrieves a block by its ID.

        Args:
            block_id (int): ID of the block.

        Returns:
            DataBlock: The block data.
        """
        if len(self.offsets) <= block_id:
            raise ParsingError("Invalid block ID.")

        addr = self.offsets[block_id]
        offset = addr & ~0x1F
        size = 1 << (addr & 0x1F)

        return DataBlock(self.block.offset_read(size, offset + 4), debug=self.logger.enable_debug)


if __name__ == "__main__":
    with open(".DS_Store", "rb") as f:
        data = f.read()

    parser = DS_Store(data, debug=True)
    filenames = parser.traverse(parser.toc["DSDB"])
    print("Extracted filenames:", filenames)
