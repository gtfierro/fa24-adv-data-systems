import os
import random
import struct
import pickle
from line_profiler import profile

class TupleStorageSystem:
    def __init__(self, schema, page_size=10000):
        self.schema = schema  # Schema is a list of field names and types
        self.page_size = page_size  # Number of tuples per page
        self.pages_dir = 'pages'  # Directory to store pages
        os.makedirs(self.pages_dir, exist_ok=True)

        # Initialize field values storage
        self.field_values = {field: [] for field in schema}
        self.tuple_ids = []
        self.current_tuple_count = 0

    def generate_random_tuple(self):
        tuple_data = []
        for field in self.schema:
            if field == 'name':
                tuple_data.append(''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=4)))
            elif field == 'age':
                tuple_data.append(random.randint(18, 65))
            elif field == 'salary':
                tuple_data.append(random.randint(30000, 120000))
        return tuple_data

    def add_tuple(self, tuple_data):
        tuple_id = self.current_tuple_count
        self.tuple_ids.append(tuple_id)
        for field, value in zip(self.schema, tuple_data):
            self.field_values[field].append(value)
        self.current_tuple_count += 1

        # If page is full, write to disk
        if self.current_tuple_count % self.page_size == 0:
            self.write_page()

    def write_page(self):
        page_id = len(os.listdir(self.pages_dir))
        # Serialize the current tuple ids
        with open(os.path.join(self.pages_dir, f'page_{page_id}.pkl'), 'wb') as f:
            pickle.dump(self.tuple_ids[-self.page_size:], f)

    def create_tuples(self, n):
        for _ in range(n):
            random_tuple = self.generate_random_tuple()
            self.add_tuple(random_tuple)
        # Write any remaining tuples if the last page isn't full
        if len(self.tuple_ids) % self.page_size != 0:
            self.write_page()

    def load_page(self, page_id):
        with open(os.path.join(self.pages_dir, f'page_{page_id}.pkl'), 'rb') as f:
            tuple_ids = pickle.load(f)
        return tuple_ids

    def select(self, predicate):
        satisfying_ids = []
        for tuple_id in self.tuple_ids:
            tuple_data = self.get_tuple_data(tuple_id)
            if predicate(tuple_data):
                satisfying_ids.append(tuple_id)
        return satisfying_ids

    def select_tid(self, predicate):
        satisfying_ids = []
        for tuple_id in self.tuple_ids:
            tuple_data = self.get_tuple_data(tuple_id)
            if predicate(tuple_data):
                satisfying_ids.append(tuple_id)
        return satisfying_ids

    def get_tuple_data(self, tuple_id):
        return [self.field_values[field][tuple_id] for field in self.schema]

    def find_field_id(self, field_name, value):
        if field_name not in self.schema:
            return -1  # Not found

        field_index = self.schema.index(field_name)
        for i, val in enumerate(self.field_values[field_name]):
            if val == value:
                return i  # Return the tuple ID of the found value
        return -1  # Not found

@profile
def number_threshold_one_field(storage):
    # Example predicate: Select tuples where age is over 30
    result = storage.select(lambda tup: tup[1] > 30)
    print("Selected Tuple IDs:", len(result))


@profile
def number_threshold_two_fields(storage):
    # Example predicate: Select tuples where salary is over 80000 and age is over 40
    result = storage.select(lambda tup: tup[2] > 80000 and tup[1] > 40)
    print("Selected Tuple IDs:", len(result))


@profile
def string_exact_match(storage):
    # Example predicate: Select tuples where name is 'JOHN'
    field_id = storage.find_field_id('name', 'JOHN')
    print("Field ID for name 'JOHN':", field_id)

    result = storage.select_tid(lambda tup: tup[0] == field_id)
    print("Selected Tuple IDs:", len(result))


@profile
def string_prefix_match(storage):
    # Example predicate: Select tuples where name starts with 'J'
    result = storage.select(lambda tup: tup[0].startswith('J'))
    print("Selected Tuple IDs:", len(result))

# Example usage
schema = ['name', 'age', 'salary']
storage = TupleStorageSystem(schema)
storage.create_tuples(10_000_000)

number_threshold_one_field(storage)
number_threshold_two_fields(storage)
string_exact_match(storage)
string_prefix_match(storage)
