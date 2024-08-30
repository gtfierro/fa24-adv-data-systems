from heapfile import Relation, DiskManager, Record
import time

# Define the relations
emp_schema = [('emp_id', 'int'), ('name', 'string'), ('salary', 'int')]
dept_schema = [('dept_no', 'int'), ('dept_name', 'string'), ('manager_id', 'int')]
works_in_schema = [('emp_id', 'int'), ('dept_no', 'int')]

employee_relation = Relation('Employee', emp_schema)
department_relation = Relation('Department', dept_schema)
works_in_relation = Relation('WorksIn', works_in_schema)

disk_manager = DiskManager()

# Join function implementations

def nested_loop_join(relation1, relation2, predicate):
    for record1 in disk_manager.scan(relation1):
        for record2 in disk_manager.scan(relation2):
            if predicate(record1, record2):
                yield (record1, record2)

def join_employee_worksin_department():
    for emp_record in disk_manager.scan(employee_relation):
        for works_record in disk_manager.scan(works_in_relation):
            if emp_record.values[0] == works_record.values[0]:
                for dept_record in disk_manager.scan(department_relation):
                    if works_record.values[1] == dept_record.values[0]:
                        yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_employee_worksin_department_deptindex():
    for emp_record in disk_manager.scan(employee_relation):
        for works_record in disk_manager.scan(works_in_relation):
            if emp_record.values[0] == works_record.values[0]:
                for dept_record in disk_manager.scan_index(department_relation, lambda record: record.values[0] == works_record.values[1], "search", works_record.values[1]):
                    yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_workin_employee_department():
    for works_record in disk_manager.scan(works_in_relation):
        for emp_record in disk_manager.scan(employee_relation):
            if works_record.values[0] == emp_record.values[0]:
                for dept_record in disk_manager.scan(department_relation):
                    if works_record.values[1] == dept_record.values[0]:
                        yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_workin_employee_department_empindex():
    for works_record in disk_manager.scan(works_in_relation):
        condition = lambda record: record.values[0] == works_record.values[0]
        for emp_record in disk_manager.scan_index(employee_relation, condition, "search", works_record.values[0]):
            for dept_record in disk_manager.scan(department_relation):
                if works_record.values[1] == dept_record.values[0]:
                    yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_workin_department_employee():
    for works_record in disk_manager.scan(works_in_relation):
        for dept_record in disk_manager.scan(department_relation):
            if works_record.values[1] == dept_record.values[0]:
                for emp_record in disk_manager.scan(employee_relation):
                    if works_record.values[0] == emp_record.values[0]:
                        yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_workin_department_employee_empindex():
    for works_record in disk_manager.scan(works_in_relation):
        for dept_record in disk_manager.scan(department_relation):
            if works_record.values[1] == dept_record.values[0]:
                for emp_record in disk_manager.scan_index(employee_relation, lambda record: record.values[0] == works_record.values[0], "search", works_record.values[0]):
                    yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_workin_department_employee_deptindex_empindex():
    for works_record in disk_manager.scan(works_in_relation):
        for dept_record in disk_manager.scan_index(department_relation, lambda record: record.values[0] == works_record.values[1], "search", works_record.values[1]):
            for emp_record in disk_manager.scan_index(employee_relation, lambda record: record.values[0] == works_record.values[0], "search", works_record.values[0]):
                yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_department_workin_employee():
    for dept_record in disk_manager.scan(department_relation):
        for works_record in disk_manager.scan(works_in_relation):
            if dept_record.values[0] == works_record.values[1]:
                for emp_record in disk_manager.scan(employee_relation):
                    if emp_record.values[0] == works_record.values[0]:
                        yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_department_workin_employee_workindex():
    for dept_record in disk_manager.scan(department_relation):
        condition = lambda record: record.values[1] == dept_record.values[0]
        for works_record in disk_manager.scan_index(works_in_relation, condition, "search", dept_record.values[0]):
            if dept_record.values[0] == works_record.values[1]:
                for emp_record in disk_manager.scan(employee_relation):
                    if emp_record.values[0] == works_record.values[0]:
                        yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

def join_department_workin_employee_workindex_empindex():
    for dept_record in disk_manager.scan(department_relation):
        condition = lambda record: record.values[1] == dept_record.values[0]
        for works_record in disk_manager.scan_index(works_in_relation, condition, "search", dept_record.values[0]):
            for emp_record in disk_manager.scan_index(employee_relation, lambda record: record.values[0] == works_record.values[0], "search", works_record.values[0]):
                yield (emp_record.values[0], emp_record.values[1], works_record.values[1])

# Benchmark join performance
def benchmark_join(join_func, join_name):
    start_time = time.time()
    records = list(join_func())
    end_time = time.time()
    print(f"{join_name}: {end_time - start_time} seconds with {len(records)} records")

def benchmark_join_formatted(join_func, join_name, trials):
    total_time = 0
    for _ in range(trials):
        start_time = time.time()
        records = list(join_func())
        end_time = time.time()
        total_time += end_time - start_time
    res = total_time / trials
    print(f"{join_name:<60}: {res:.2f} seconds with {len(records)} records")


join_functions = [
    (join_employee_worksin_department, "Employee, WorksIn, Department"),
    (join_employee_worksin_department_deptindex, "Employee, WorksIn, Department (WorksIn, Department Index)"),
    (join_workin_employee_department, "WorksIn, Employee, Department"),
    (join_workin_employee_department_empindex, "WorksIn, Employee, Department (Employee Index)"),
    (join_workin_department_employee, "WorksIn, Department, Employee"),
    (join_workin_department_employee_empindex, "WorksIn, Department, Employee (Employee Index)"),
    (join_workin_department_employee_deptindex_empindex, "WorksIn, Department, Employee (Department, Employee Index)"),
    (join_department_workin_employee, "Department, WorksIn, Employee"),
    (join_department_workin_employee_workindex, "Department, WorksIn, Employee (Indexed)"),
    (join_department_workin_employee_workindex_empindex, "Department, WorksIn, Employee (WorksIn, Employee Index)"),
]

for join_function, join_name in join_functions:
    benchmark_join_formatted(join_function, join_name, 10)
