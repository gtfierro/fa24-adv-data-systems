import os
import struct
import itertools
from typing import List, Tuple, Generator, Callable

# Constants
CHAR_SIZE = 32
BOOL_SIZE = 1
INT_SIZE = 4
MAX_RECORDS_PER_PAGE = 100

class Record:
    def __init__(self, relation_name: str, record_id: int, values: Tuple):
        self.relation_name = relation_name
        self.record_id = record_id
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

    def _get_heap_file_path(self, relation_name: str, index: int) -> str:
        return os.path.join(self.heap_dir, f"{relation_name}_{index}.heap")

    def _open_heap_file(self, path: str):
        if self.current_heap_file:
            self.current_heap_file.close()
        self.current_heap_file = open(path, 'ab+')

    def _create_new_page(self, relation: Relation) -> Page:
        return Page([])

    def insert_record(self, relation: Relation, values: Tuple) -> int:
        record_length = relation.record_length()
        record_id = 0

        # Find the first page with space
        for i in itertools.count():
            path = self._get_heap_file_path(relation.name, i)
            if not os.path.exists(path):
                self._open_heap_file(path)
                page = self._create_new_page(relation)
                break
            else:
                self._open_heap_file(path)
                self.current_heap_file.seek(0, os.SEEK_END)
                if self.current_heap_file.tell() < MAX_RECORDS_PER_PAGE * record_length:
                    page = self._create_new_page(relation)
                    break

        record_id = self.current_heap_file.tell() // record_length
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
        record = Record(relation.name, 0, tuple(values))
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
