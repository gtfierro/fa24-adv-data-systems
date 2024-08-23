from tqdm import tqdm
from line_profiler import profile
import os
import random
import struct
import argparse
from typing import Generator, List, Tuple

class Relation:
    def __init__(self, name: str, schema: List[str], N: int = 100000):
        self.name = name
        self.schema = schema
        self.relation_dir = "relations"
        self.tuple_dir = "tuples"
        self.field_dir = "fields"
        self.N = N

    @profile
    def scan(self):
        """Scans the relation and returns a generator of TIDs."""
        for i in tqdm(range(self.N)):
            page_num = i >> 22
            relation_file = os.path.join(self.relation_dir, f"{page_num}.dat")
            with open(relation_file, 'rb') as rf:
                rf.seek(i * 4)
                tid = struct.unpack('I', rf.read(4))[0]
                yield tid

    @profile
    def get_tuple(self, tid: int) -> Tuple[int]:
        """Returns the tuple data for the provided TID."""
        page_num = tid >> 22
        offset = tid & 0x3FFFFF
        tuple_file = os.path.join(self.tuple_dir, f"{page_num}.dat")
        with open(tuple_file, 'rb') as tf:
            tf.seek(offset * 4)
            tuple_data = struct.unpack('III', tf.read(12))
        return tuple_data

    @profile
    def get_field_value(self, field_name: str, offset: int) -> int:
        """Returns the field value at the provided offset."""
        field_file = os.path.join(self.field_dir, f"{field_name}.dat")
        with open(field_file, 'rb') as ff:
            ff.seek(offset * 4)
            value = struct.unpack('I', ff.read(4))[0]
        return value

    @profile
    def get_tuple_values(self, tid: int) -> List[int]:
        """Returns a list of field values for the provided TID."""
        tuple_data = self.get_tuple(tid)
        return [self.get_field_value(field, offset) for field, offset in zip(self.schema, tuple_data)]

    def save_field_value(self, field_name: str, value: int) -> int:
        """Saves the field value in a separate file."""
        field_file = os.path.join(self.field_dir, f"{field_name}.dat")
        with open(field_file, 'ab') as ff:
            offset = os.path.getsize(field_file) // 4
            ff.write(struct.pack('I', value))
        return offset

    def save_tuple(self, idx: int, tuple_data: List[int]) -> int:
        """Saves the tuple data to the tuple file. Returns TID"""
        page_num = idx >> 22
        offset = idx & 0x3FFFFF
        tuple_file = os.path.join(self.tuple_dir, f"{page_num}.dat")
        with open(tuple_file, 'ab') as tf:
            offset = os.path.getsize(tuple_file) // 4
            tf.seek(offset * 4)
            tf.write(struct.pack('III', *tuple_data))
        return page_num << 22 | offset

    def generate(self) -> None:
        """Generates N tuples of random data for the provided schema."""
        os.makedirs("relations", exist_ok=True)
        os.makedirs("tuples", exist_ok=True)
        os.makedirs("fields", exist_ok=True)
        for i in tqdm(range(self.N)):
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
                field_addr = self.save_field_value(field, value)
                tuple_data.append(field_addr)

            tid = self.save_tuple(i, tuple_data)

            # save the tid to the list of tids for the relation
            relation_file = os.path.join("relations", f"{page_num}.dat")
            with open(relation_file, 'ab') as rf:
                rf.write(struct.pack('I', tid))


@profile
def find_with_value(relation: Relation, field_name: str, value: int) -> Generator[int, None, None]:
    """Finds all tuples with the specified field value."""
    for tid in relation.scan():
        values = relation.get_tuple_values(tid)
        if values[relation.schema.index(field_name)] == value:
            yield tid


def main():
    parser = argparse.ArgumentParser(description="Manage Relations")
    subparsers = parser.add_subparsers(dest='command')

    # Create subcommand
    create_parser = subparsers.add_parser('create', help='Create a new relation')
    create_parser.add_argument('--N', type=int, default=100000, help='Number of tuples to generate')

    # Benchmark subcommand (currently does nothing)
    bench_parser = subparsers.add_parser('bench', help='Run a benchmark (currently does nothing)')
    bench_parser.add_argument('--N', type=int, default=100000, help='Number of tuples to generate')

    args = parser.parse_args()
    name = "employee"
    schema = ["employee_id", "age", "salary"]

    if args.command == 'create':
        relation = Relation(name, schema, args.N)
        relation.generate()
        print(f"Relation with {args.N} tuples.")

    elif args.command == 'bench':
        relation = Relation(name, schema, args.N)
        print(len(list(find_with_value(relation, 'age', 30))))


if __name__ == '__main__':
    main()
