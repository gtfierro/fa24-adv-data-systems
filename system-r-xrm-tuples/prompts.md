Generate a Python implementation of a tuple-based storage system.

A relation consists of a set of tuple IDs. Tuple IDs are 32-bit numbers.
The first 10 bits of a tuple ID are the page number, and the last 22 bits are memory offset within the page.
Assume fixed-length fields.

Each tuple ID is a memory address of a tuple. The tuple is stored in memory as a sequence of bytes.
A tuple has a fixed number of fields, each of a fixed length. The fields are stored in order, with no padding between fields.
Each field is a 32-bit integer representing the address of the field's value in memory.
The first 10 bits of the field address are the page number, and the last 22 bits are memory offset within the page.
Each attribute of the relation has a separate array of values. The field address is the index of the value in the array.


Write a method which takes a description of a relational schema, and a number N, and generates N tuples of random data for the schema.
Save the tuples to disk in pages of 10000 tuples each.
Generate data 1 page at a time. When a page is full, write it to disk.
When reading a page, load it into memory.
Save all pages in a directory named "pages".

Save the field values in separate files, one per field.
These need to be loaded into memory when reading a page.

Write a 'select' method which takes a predicate and returns a list of tuple IDs which satisfy the predicate.
This method dereferences the tuple IDs to get the actual tuple data.
Write a 'select_tid' method which takes a predicate and returns a list of tuple IDs which satisfy the predicate.
This method does not dereference the tuple IDs.
Write a method that turns a list of tuple IDs into a list of tuples.
Write a method which finds the field ID for a given field name and value.

Create a sample relation R(name, age, salary) with 100000 tuples.
Name is char(4), age is int, and salary is int.
