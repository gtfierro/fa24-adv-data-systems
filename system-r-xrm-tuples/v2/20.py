import os
import random
import struct
from typing import Generator, List, Tuple

class Relation:
    def __init__(self, name: str, schema: List[str], N: int = 100000):
        self.name = name
        self.schema = schema
        self.relation_dir = "relations"
        self.tuple_dir = "tuples"
        self.field_dir = "fields"
        self.N = N
        self.generate()

    def scan(self):
        """Scans the relation and returns a generator of TIDs."""
        for i in range(self.N):
            page_num = i >> 22
            relation_file = os.path.join(self.relation_dir, f"{page_num}.dat")
            with open(relation_file, 'rb') as rf:
                rf.seek((i & 0x3FFFFF) * 4)
                tid = struct.unpack('I', rf.read(4))[0]
                yield tid

    def get_tuple(self, tid: int) -> Tuple[int]:
        """Returns the tuple data for the provided TID."""
        page_num = tid >> 22
        offset = tid & 0x3FFFFF
        tuple_file = os.path.join(self.tuple_dir, f"{page_num}.dat")
        with open(tuple_file, 'rb') as tf:
            tf.seek(offset * 4)
            tuple_address = struct.unpack('I', tf.read(4))[0]
            tf.seek(tuple_address * 4)
            tuple_data = struct.unpack('III', tf.read(12))
        return tuple_data

    def save_field_value(self, field_name: str, value: int) -> int:
        """Saves the field value in a separate file."""
        field_file = os.path.join(self.field_dir, f"{field_name}.dat")
        with open(field_file, 'ab') as ff:
            ff.write(struct.pack('I', value))
        return os.path.getsize(field_file) // 4

    def generate(self) -> None:
        """Generates N tuples of random data for the provided schema."""
        os.makedirs("relations", exist_ok=True)
        os.makedirs("tuples", exist_ok=True)
        os.makedirs("fields", exist_ok=True)
        tuple_address = 0
        for i in range(self.N):
            page_num = i >> 22  # 10 bits for page number

            # generate the data for this tuple
            tuple_data = []
            for field in self.schema:
                if field == 'employee_id':
                    value = random.randint(1, 100000)
                elif field == 'age':
                    value = random.randint(18, 65)
                elif field == 'salary':
                    value = random.randint(30000, 120000)
                tuple_data.append(value)
                self.save_field_value(field, value)

            # save the tuple_data to the tuple file. Use the page number as the file name, 
            # and the offset as the position in the file
            tuple_file = os.path.join("tuples", f"{page_num}.dat")
            with open(tuple_file, 'ab') as tf:
                tf.write(struct.pack('I', tuple_address))
                for value in tuple_data:
                    tf.write(struct.pack('I', value))
                tuple_address += 1
            offset = os.path.getsize(tuple_file) // 4 - 1

            tid = (page_num << 22) | offset

            # save the tid to the list of tids for the relation
            relation_file = os.path.join("relations", f"{page_num}.dat")
            with open(relation_file, 'ab') as rf:
                rf.write(struct.pack('I', tid))


# Example usage
relation_name = "employee"
relation_schema = ["employee_id", "age", "salary"]
relation = Relation(relation_name, relation_schema, 100_000)
# Scan the relation and print TIDs
for tid in relation.scan():
    print(tid)
    print(relation.get_tuple(tid))
