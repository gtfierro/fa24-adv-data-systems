import random
import string
import time
from heapfile import DiskManager, Relation, Record
import os
from bptree import BPlusTreeNode

# Create a new relation R(name, age)
relation_name = "R"
relation_schema = [("name", "string"), ("age", "int")]
relation = Relation(relation_name, relation_schema)

# Initialize DiskManager
disk_manager = DiskManager()

# Insert 10000 random records
for _ in range(10000):
    name = ''.join(random.choices(string.ascii_uppercase, k=4))
    age = random.randint(0, 100)
    disk_manager.insert_record(relation, (name, age))

# Define scan_all_heap method
def scan_all_heap(disk_manager: DiskManager, relation: Relation) -> int:
    count = 0
    for _ in disk_manager.scan(relation):
        count += 1
    return count

# Define scan_all_heap_predicate method
def scan_all_heap_predicate(disk_manager: DiskManager, relation: Relation) -> int:
    count = 0
    for _ in disk_manager.scan(relation, predicate=lambda record: record.values[1] > 50):
        count += 1
    return count

# Define scan_all_index method
def scan_all_index(disk_manager: DiskManager, relation: Relation) -> int:
    count = 0
    index_path = os.path.join(disk_manager.heap_dir, f"{relation.name}.idx")
    
    with open(index_path, 'rb') as index_file:
        tree = disk_manager._deserialize_bplustree(index_file)
        
        def count_records(node: BPlusTreeNode):
            nonlocal count
            count += len(node.keys)
            for child in node.children:
                count_records(child)
        
        count_records(tree.root)
    
    return count

def scan_all_index_predicate(disk_manager: DiskManager, relation: Relation, predicate): # yield records that satisfy the predicate
    index_path = os.path.join(disk_manager.heap_dir, f"{relation.name}.idx")
    with open(index_path, 'rb') as index_file:
        tree = disk_manager._deserialize_bplustree(index_file)
        
        def scan_records(node: BPlusTreeNode):
            if node.is_leaf:
                for key, record_id in node.keys:
                    record = disk_manager.get_record(relation, record_id)
                    if predicate(record):
                        yield record
            else:
                for child in node.children:
                    yield from scan_records(child)
        
        yield from scan_records(tree.root)


# Define scan_all_index_predicate method
def scan_all_index_predicate_50(disk_manager: DiskManager, relation: Relation) -> int:
    count = 0
    column_index = next(i for i, (name, _) in enumerate(relation.schema) if name == "age")
    
    index_path = os.path.join(disk_manager.heap_dir, f"{relation.name}.idx")
    
    with open(index_path, 'rb') as index_file:
        tree = disk_manager._deserialize_bplustree(index_file)
        
        def count_records(node: BPlusTreeNode):
            nonlocal count
            if node.is_leaf:
                count += sum(1 for key, _ in node.keys if key > 50)
            else:
                for child in node.children:
                    count_records(child)
        
        count_records(tree.root)
    
    return count



# Benchmark scan_all_heap
start_time = time.time()
total_records_heap = scan_all_heap(disk_manager, relation)
elapsed_time = time.time() - start_time
print(f"scan_all_heap: {total_records_heap} records, Time: {elapsed_time} seconds")

# Benchmark scan_all_heap_predicate
start_time = time.time()
total_records_heap_predicate = scan_all_heap_predicate(disk_manager, relation)
elapsed_time = time.time() - start_time
print(f"scan_all_heap_predicate: {total_records_heap_predicate} records, Time: {elapsed_time} seconds")

# Create index on age column
disk_manager.make_index(relation, "age")

# Benchmark scan_all_index
start_time = time.time()
total_records_index = scan_all_index(disk_manager, relation)
elapsed_time = time.time() - start_time
print(f"scan_all_index: {total_records_index} records, Time: {elapsed_time} seconds")

# Benchmark scan_all_index_predicate
start_time = time.time()
total_records_index_predicate = scan_all_index_predicate_50(disk_manager, relation)
elapsed_time = time.time() - start_time
print(f"scan_all_index_predicate_50: {total_records_index_predicate} records, Time: {elapsed_time} seconds")
