from tqdm import tqdm
from heapfile import Relation, DiskManager
import random
import string

# Define the relations with emp_id added
emp_schema = [('emp_id', 'int'), ('name', 'string'), ('salary', 'int')]
dept_schema = [('dept_no', 'int'), ('dept_name', 'string'), ('manager_id', 'int')]
works_in_schema = [('emp_id', 'int'), ('dept_no', 'int')]

employee_relation = Relation('Employee', emp_schema)
department_relation = Relation('Department', dept_schema)
works_in_relation = Relation('WorksIn', works_in_schema)

disk_manager = DiskManager()

# Helper Function to generate random strings
def random_string(length=10):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Insert data
employee_ids = []
for emp_id in tqdm(range(1, 1001)):  # Starting emp_id from 1 to 10000
    name = random_string()
    salary = random.randint(10000, 100000)
    disk_manager.insert_record(employee_relation, (emp_id, name, salary))
    employee_ids.append(emp_id)  # Storing emp_id instead of name for later use
disk_manager.make_index(employee_relation, 'emp_id')

department_numbers = list(range(1, 21))
for dept_no in department_numbers:
    dept_name = f"Department_{dept_no}"
    manager_id = random.choice(employee_ids)
    disk_manager.insert_record(department_relation, (dept_no, dept_name, manager_id))

# create index on department.dept_no
disk_manager.make_index(department_relation, 'dept_no')

for emp_id in tqdm(employee_ids):
    dept_no = random.choice(department_numbers)
    disk_manager.insert_record(works_in_relation, (emp_id, dept_no))
disk_manager.make_index(works_in_relation, 'dept_no')
