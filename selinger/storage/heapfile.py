import os
import struct
import itertools
from typing import List, Tuple, Generator, Callable
from bptree import BPlusTree, BPlusTreeNode

# Constants
CHAR_SIZE = 32
BOOL_SIZE = 1
INT_SIZE = 4
MAX_RECORDS_PER_PAGE = 100


class Record:
    def __init__(self, relation_name: str, record_id: int, values: Tuple):
        self.relation_name = relation_name
        self.record_id = record_id  # record_id now has a special format
        self.values = values


class Relation:
    def __init__(self, name: str, schema: List[Tuple[str, str]]):
        self.name = name
        self.schema = schema

    def record_length(self) -> int:
        length = 0
        for _, typ in self.schema:
            if typ == 'string':
                length += CHAR_SIZE
            elif typ == 'bool':
                length += BOOL_SIZE
            elif typ == 'int':
                length += INT_SIZE
        return length


class Page:
    def __init__(self, records: List[Record]):
        self.records = records

    def has_space(self, record_length: int) -> bool:
        return len(self.records) < MAX_RECORDS_PER_PAGE


class DiskManager:
    def __init__(self):
        self.heap_dir = 'heap'
        self.current_heap_file = None
        self.current_page_index = -1

    def _get_heap_file_path(self, relation_name: str, index: int) -> str:
        return os.path.join(self.heap_dir, f"{relation_name}_{index}.heap")

    def _open_heap_file(self, path: str):
        if self.current_heap_file:
            self.current_heap_file.close()
        self.current_heap_file = open(path, 'ab+')

    def _create_new_page(self, relation: Relation) -> Page:
        return Page([])

    def get_record(self, relation: Relation, record_id: int) -> Record:
        # Extract page index and record offset from the record_id
        page_index = record_id >> 22  # Get the first 10 bits for the page index
        record_offset = record_id & ((1 << 22) - 1)  # Get the last 22 bits for the offset

        # Open the correct heap file for the relation and page index
        heap_file_path = self._get_heap_file_path(relation.name, page_index)

        # Open the heap file in binary read mode
        with open(heap_file_path, 'rb') as heap_file:
            # Calculate the position to seek to based on the record offset
            record_length = relation.record_length()
            position = record_offset * record_length

            # Move the file pointer to the desired position
            heap_file.seek(position)

            # Read the record data
            record_data = heap_file.read(record_length)

            if not record_data:
                raise ValueError(f"Record at ID {record_id} not found.")

            # Deserialize the record
            return self._deserialize_record(relation, record_data)

    def insert_record(self, relation: Relation, values: Tuple) -> int:
        record_length = relation.record_length()

        # Find the first page with space or create a new page
        while True:
            self.current_page_index += 1
            path = self._get_heap_file_path(relation.name, self.current_page_index)
            if not os.path.exists(path):
                self._open_heap_file(path)
                page = self._create_new_page(relation)
                break
            else:
                self._open_heap_file(path)
                self.current_heap_file.seek(0, os.SEEK_END)
                if self.current_heap_file.tell() < MAX_RECORDS_PER_PAGE * record_length:
                    break

        # Calculate record offset in the current page
        record_offset = self.current_heap_file.tell() // record_length
        record_id = (self.current_page_index << 22) | record_offset  # First 10 bits are page number, last 22 bits are offset

        # Create the record
        record = Record(relation.name, record_id, values)

        # Insert record
        self.current_heap_file.write(self._serialize_record(relation, record))

        return record_id

    def _serialize_record(self, relation: Relation, record: Record) -> bytes:
        serialized = bytearray()
        for (attr_name, attr_type), value in zip(relation.schema, record.values):
            if attr_type == 'string':
                serialized.extend(struct.pack(f'{CHAR_SIZE}s', value.encode('utf-8')))
            elif attr_type == 'bool':
                serialized.extend(struct.pack('?', value))
            elif attr_type == 'int':
                serialized.extend(struct.pack('i', value))
        return serialized

    def scan(self, relation: Relation, predicate: Callable[[Record], bool] = None) -> Generator[Record, None, None]:
        for file_name in self.list_files():
            if file_name.startswith(relation.name):
                path = self._get_heap_file_path(relation.name, int(file_name.split('_')[1].split('.')[0]))
                self._open_heap_file(path)
                self.current_heap_file.seek(0)

                while True:
                    record_data = self.current_heap_file.read(relation.record_length())
                    if not record_data:
                        break
                    record = self._deserialize_record(relation, record_data)
                    if predicate is None or predicate(record):
                        yield record

    def scan_index(self, relation: Relation, predicate: Callable[[Record], bool], scan_type: str, *args) -> Generator[Record, None, None]:
        index_file_path = os.path.join(self.heap_dir, f"{relation.name}.idx")

        if not os.path.exists(index_file_path):
            raise ValueError(f"Index file for relation {relation.name} not found.")

        # Deserialize the BPlusTree from the index file
        with open(index_file_path, 'rb') as index_file:
            index_tree = self._deserialize_bplustree(index_file)

        print(f"Scanning index for relation {relation.name} using {scan_type} with args {args}")
        if scan_type == "scan":
            records = index_tree.scan()
        elif scan_type == "search":
            if len(args) != 1:
                raise ValueError("Search requires exactly one argument: the value to search for.")
            value = args[0]
            records = index_tree.search(value)
        elif scan_type == "range_search":
            if len(args) != 2:
                raise ValueError("Range search requires exactly two arguments: low and high values.")
            low, high = args
            records = index_tree.range_search(low, high)
        else:
            raise ValueError(f"Unsupported scan type: {scan_type}")

        # Now retrieve the actual records corresponding to the found record_ids
        for record_id in records:
            record = self.get_record(relation, record_id)
            if predicate is None or predicate(record):
                yield record

    def _deserialize_record(self, relation: Relation, data: bytes) -> Record:
        offset = 0
        values = []
        for attr_name, attr_type in relation.schema:
            if attr_type == 'string':
                value = struct.unpack_from(f'{CHAR_SIZE}s', data, offset)[0].decode('utf-8').strip('\x00')
                offset += CHAR_SIZE
            elif attr_type == 'bool':
                value = struct.unpack_from('?', data, offset)[0]
                offset += 1
            elif attr_type == 'int':
                value = struct.unpack_from('i', data, offset)[0]
                offset += 4
            values.append(value)

        # Decode the record_id back to page index and offset if needed
        record_id = 0  # Placeholder (usually you'd fetch this from your record)
        record = Record(relation.name, record_id, tuple(values))
        return record

    def list_files(self) -> List[str]:
        if not os.path.exists(self.heap_dir):
            os.makedirs(self.heap_dir)
        return [f for f in os.listdir(self.heap_dir) if f.endswith('.heap')]

    def get_file(self, file_name: str) -> Page:
        relation_name = file_name.split('_')[0]
        index = int(file_name.split('_')[1].split('.')[0])
        path = self._get_heap_file_path(relation_name, index)
        with open(path, 'rb') as file:
            data = file.read()
        return Page(data)

    def make_index(self, relation: Relation, column_name: str):
        column_index = next(i for i, (name, _) in enumerate(relation.schema) if name == column_name)
        index = BPlusTree()

        file_path = os.path.join(self.heap_dir, f"{relation.name}.idx")
        with open(file_path, 'wb') as index_file:
            for record in self.scan(relation):
                value = record.values[column_index]
                index.insert(value, record.record_id)

        with open(file_path, 'wb') as index_file:
            self._serialize_bplustree(index, index_file)

    def _serialize_bplustree(self, tree: BPlusTree, file):
        def _recurse_serialize(node: BPlusTreeNode):
            file.write(b'\x01' if node.is_leaf else b'\x00')
            file.write(struct.pack('i', len(node.keys)))
            for key, record_id in node.keys:
                file.write(struct.pack('i', key))
                file.write(struct.pack('i', record_id))
            file.write(struct.pack('i', len(node.children)))
            for child in node.children:
                _recurse_serialize(child)

        _recurse_serialize(tree.root)

    def _deserialize_bplustree(self, file) -> BPlusTree:
        def _recurse_deserialize() -> BPlusTreeNode:
            is_leaf = struct.unpack('b', file.read(1))[0] == 1
            num_keys = struct.unpack('i', file.read(4))[0]
            keys = []
            for _ in range(num_keys):
                key = struct.unpack('i', file.read(4))[0]
                record_id = struct.unpack('i', file.read(4))[0]
                keys.append((key, record_id))

            num_children = struct.unpack('i', file.read(4))[0]
            children = []
            for _ in range(num_children):
                children.append(_recurse_deserialize())

            node = BPlusTreeNode(is_leaf=is_leaf)
            node.keys = keys
            node.children = children
            return node

        tree = BPlusTree()
        tree.root = _recurse_deserialize()
        return tree
