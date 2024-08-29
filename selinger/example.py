from heapfile import Relation, DiskManager
import random
import time

# Use the provided classes and methods
relation = Relation('R', [('name', 'string'), ('age', 'int')])
disk_manager = DiskManager()

# Insert 10000 random records
for _ in range(10000):
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=10))
    age = random.randint(0, 100)
    disk_manager.insert_record(relation, (name, age))

# Method to scan all records in the heap file and count them
def scan_all_heap() -> int:
    count = 0
    for _ in disk_manager.scan(relation):
        count += 1
    return count

# Method to scan all records in the heap file that satisfy the predicate age > 50 and count them
def scan_all_predicate() -> int:
    count = 0
    for record in disk_manager.scan(relation, predicate=lambda rec: rec.values[1] > 50):
        count += 1
    return count

# Benchmarking
start_time = time.time()
total_records = scan_all_heap()
print(f"Total records scanned: {total_records}")
print(f"Time taken to scan all records: {time.time() - start_time} seconds")

start_time = time.time()
predicate_records = scan_all_predicate()
print(f"Total records with age > 50: {predicate_records}")
print(f"Time taken to scan records with age > 50: {time.time() - start_time} seconds")
