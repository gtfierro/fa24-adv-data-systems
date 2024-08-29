from tqdm import tqdm
from heapfile import Relation, DiskManager
import random
import string

# Define the relations
emp_schema = [('name', 'string'), ('salary', 'int')]
dept_schema = [('dept_no', 'int'), ('dept_name', 'string'), ('manager_name', 'string')]
works_in_schema = [('name', 'string'), ('dept_no', 'int')]

employee_relation = Relation('Employee', emp_schema)
department_relation = Relation('Department', dept_schema)
works_in_relation = Relation('WorksIn', works_in_schema)

disk_manager = DiskManager()

# Helper Function to generate random strings
def random_string(length=10):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Insert data
employee_names = []
for _ in tqdm(range(10000)):
    name = random_string()
    salary = random.randint(10000, 100000)
    disk_manager.insert_record(employee_relation, (name, salary))
    employee_names.append(name)

department_numbers = list(range(1, 21))
for dept_no in department_numbers:
    dept_name = f"Department_{dept_no}"
    manager_name = random.choice(employee_names)
    disk_manager.insert_record(department_relation, (dept_no, dept_name, manager_name))
# create index on department.dept_no
disk_manager.make_index(department_relation, 'dept_no')

for name in tqdm(employee_names):
    dept_no = random.choice(department_numbers)
    disk_manager.insert_record(works_in_relation, (name, dept_no))
