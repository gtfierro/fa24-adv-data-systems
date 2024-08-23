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
        self.relation_dir = "phase1/relations"
        self.tuple_dir = "phase1/tuples"
        self.N = N

    @profile
    def scan(self):
        """Scans the relation and returns a generator of TIDs."""
        for i in tqdm(range(self.N)):
            page_num = i >> 16
            offset = i & 0xFFFF
            relation_file = os.path.join(self.relation_dir, f"{page_num}.dat")
            with open(relation_file, 'rb') as rf:
                rf.seek(offset * 4)
                tid = struct.unpack('I', rf.read(4))[0]
                yield tid

    @profile
    def get_tuple(self, tid: int) -> Tuple[int]:
        """Returns the tuple data for the provided TID."""
        page_num = tid >> 16
        offset = tid & 0xFFFF
        tuple_file = os.path.join(self.tuple_dir, f"{page_num}.dat")
        with open(tuple_file, 'rb') as tf:
            tf.seek(offset * (4 * len(self.schema)))  # Assume each int is 4 bytes
            tuple_data = struct.unpack('I' * len(self.schema), tf.read(4 * len(self.schema)))
        return tuple_data

    @profile
    def get_tuple_values(self, tid: int) -> List[int]:
        """Returns a list of field values for the provided TID."""
        return list(self.get_tuple(tid))

    def save_tuple(self, idx: int, tuple_data: List[int]) -> int:
        """Saves the tuple data to the tuple file. Returns TID"""
        page_num = idx >> 16
        tuple_file = os.path.join(self.tuple_dir, f"{page_num}.dat")
        with open(tuple_file, 'ab') as tf:
            offset = os.path.getsize(tuple_file) // (4 * len(self.schema))
            tf.seek(offset * (4 * len(self.schema)))
            tf.write(struct.pack('I' * len(tuple_data), *tuple_data))
        return page_num << 16 | offset

    def generate(self) -> None:
        """Generates N tuples of random data for the provided schema."""
        os.makedirs("phase1/relations", exist_ok=True)
        os.makedirs("phase1/tuples", exist_ok=True)
        for i in tqdm(range(self.N)):
            page_num = i >> 16  # 10 bits for page number

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

            tid = self.save_tuple(i, tuple_data)

            # save the tid to the list of tids for the relation
            relation_file = os.path.join("phase1/relations", f"{page_num}.dat")
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
    bench_parser = subparsers.add_parser('bench', help='Run a benchmark')
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
