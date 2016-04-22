# Hadoop_CustomInputFormat

Step 1(Create Datasets)
---

- In this step, you are required to create a customer dataset with the above format.
- This format is called JSON format. Some of its characteristics are the following:
    - Every record starts with “{“ and it ends with “}”
    - Each record has many fields separated by “,”.
    - Each field in a record has a name followed by “:” followed by a value.
    - The value can be either one of the simple data types, e.g., string, date, number, or an array of
values, e.g., array of numbers.
- Scale the dataset to be at least 100MBs
    - Customer Ids should be incremental unique numbers
    - Salary should be a random integer between 100 and 1000
    - Gender should be a random value of either “male” or “female”
    - Name and Address should be random strings each of length 100 character.
- Make sure within a single record, there is no “{“ or “}”. These brackets should identify the start and end of each record.

Step 2(Map Job with a Custom Input Format)
---

- Now, to do any job on the above dataset using the standard “TextInputFormat()”, the map function must be complex as it needs to collect many lines to form a single record. This complexity will repeat with each written job over the above dataset.
- A better way is to write a custom input format, call it “CustomerInputFormat” that reads many lines from the input file until it gets a complete record, and then coverts them to a list of comma separated values in a single line, and then pass it to the map function.
   - E.g., each input to the map function should be: \<id\>, \<name\>, \<addr\>, \<salary\>, \<gender\>
- Your task is to write this new “CustomerInputFormat”, and use it in a map-reduce job that aggregates the records based on the Salary field, 
and for each salary value it should report the number of males and females having this salary.
