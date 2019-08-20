# Csv Chef - Write recipes for your CSV operations

In businesses dealing with big data, we often find ourselves having to write a one-off script
to read and parse CSV files following a specific set of rules, or process the information to 
send to different third-party services.

The idea behind CSV-Chef is to enable anyone to run operations and transformations from a CSV file by 
writing a recipe and run it without having to write code.

## Example 

```yaml
# my_config.yml

# Importing custom javascript parsers
jsParsers:
  - /Users/me/jsParsers/lowercase.js

# columns definition with their parsers
cols:
  - name: id
    type: int
    not_empty: true
  
  # definition of the filename code
  # if it is empty, the default 'aaaa-1/b' will be assigned to it, and then transformed
  # to uppercase thanks to the uppercase parser. The tilt '~' in the arg value means that
  # we want to parse the current value in this column
  - name: code
    type: string
    not_empty: true
    default: aaaa-1/b
    parsers:
      - name: uppercase
        args:
          value: ~

  # definition of the filename column. It cannot be empty and will throw an error if an empty value is found
  - name: filename
    type: string
    not_empty: true
  
  # definition of the 'is_executable column of type bool.
  # Values like '0', '1', 'No', 'Yes', 'True', 'False' are accepted
  - name: is_executable
    type: bool
    default: false
    not_empty: true

  # creates a new column with the file extension taken from the value in 'filename'
  - name: extension
    type: string
    dynamic: true
    parsers:
      - name: ext
        args:
          filename: 
            col: filename

  # creates a new dynamic column called 'file_md5', reads the file path in the 'filename' column
  # and generates its file hash using the md5 alg.
  - name: file_md5
    type: string
    parsers:
        - name: fileMd5
          args:
            filename:
                col: filename
  
  # Creates a new dynamic column called 's3Path' and generates an s3 path using the concat parser.
  # It also transforms the value to lowercase using the imported 'lowercase.js' custom javascript parser
  - name: s3Path
    type: string
    dynamic: true
    parsers:
      - name: concat
        args:
          - value: "s3://my-bucket/path/to/my/loc/"
          - col: filename
      - name: lowercase.js
        args:
            col:
              value: ~

operations:
# sorting all rows by extension and filename
- name: my_first_sort_op
  operation: sort
  args:
    cols:
      values: [ext, code]
    order:
      value: [asc, desc]

# counting all duplicated combo of code and extension and the output includes
# the id, filename, code and extension with an extra dynamic column called 'dupes_count'.
# It will only output them if there were at least 1 match or more
# keepState means that we keep the output in memory so it is available to any of the following operations
- name: count_duplicates
  operation: dupesCount
  keepState: true
  args:
    indexCols:
      values: [code, extension]
    outCols:
      values: [id, filename, code, extension]
    countCol:
      value: dupes_count
    gt:
      value: 1

# Printing out the results from count_duplicates operation to the console
- name: printing_count_duplicates
  operation: print
  fromState: count_duplicates
  args:
    cols:
      values: [id, filename, code, extension, dupes_count]

# Creating a new file and writing the output of count_duplicates
- name: writing_count_duplicates_to_file
  operation: toFile
  fromState: count_duplicates
  args:
    filename:
      value: "/Users/me/Documents/dupes_count.csv"
    cols:
      values: [id, filename, code, extension, dupes_count]
```


```javascript
// /Users/me/jsParsers/lowercase.js
/**
 * col is an argument of type string that will be required to defined in the configration file
 * and is the value to transform to lowercase
 */
args = {
    'col': 'string',
};

// output a string holding the outcome of the script
output = col.toLowerCase();
```

```sh
$ csv-chef my_config.yml my_csv_file.csv
```

## Column parsers

### uppercase
```yaml
# Transforms the current column value to uppercase format
- name: uppercase
  args:
    value: ~
```

### lowercase
```yaml
# Transforms the 'code' column to lowercase format
- name: lowercase
  args:
    col: code
```

### fileMd5
```yaml
# Generates the md5 for the file path in the 'file_path' column
- name: fileMd5
  args:
    filename:
      col: file_path
```

### concat
```yaml
# Concatenates multiple values and/or columns together.
# This example would generate something like: 
# 'https://mydomain/AAAA/1234/download
- name: concat
  args:
    values:
      values:
      - value: "https://mydomain/"
      - col: code
      - value: "/"
      - col: id
      - value: download
```

### contains
```yaml
# Outputs 'true' if the value in the 'code' column contains 'BB0', else 'false'
- name: contains
  args:
    value:
      col: code
    term:
      value: "BB0"
```

### fileExists
```yaml
# Outputs 'true' if the file path in the 'filename' column actually exists in the system
# else 'false'
- name: fileExists
  args:
    filename:
      col: filename
```

### ext
```yaml
# Extracts the extension from the value in the 'filename' column
- name: ext
  args:
    filename: 
      col: filename
```

## Operations

Operations transforms the all the rows in the CSV to the desired outcome.

### sort
```yaml
# Sorts all rows in the csv by columns
- name: sort_by_id_desc
  operation: sort
  args:
      cols:
        values: [ext, id]
      order:
        values: [desc, desc]
```

### dupesCount
```yaml
# Counts the number of times the exact values combinations were found
- name: count_dupes
  operation: dupesCount
  keepState: true
  args:
    indexCols: # list of columns to compare
      values: [code]
    outCols: # columns that we want out
      values: [id, code, ext]
    countCol: # the name of the new column with the count
      value: dupes_count
    gt: # only output rows with a count greater than 1
      value: 1
```

Example output:
```csv
id,code,ext,dupes_count
1,AAA,pdf,4
2,AA1,xls,2
3,001,pdf,3
```

### findDupes
```yaml
# Find duplicates and list them in a new column separated by a given separator
- name: operation_to_find_duplicate
  operation: findDuplicates
  keepState: true
  args:
    indexCols: # the list of columns used for comparison
      values: [code, ext]
    outCols: # the columns we want out
      values: [id, filename, code, ext]
    idCol: # the name of the column holding the unique identifier
      value: id
    dupeIdsCol: # name of the column with the list of values representing the dupes
      value: similar
    sep: # separator value
      value: ";"
```

Example output:
```csv
id,filename,code,ext,similar
1,myfile1.pdf,AAA,pdf,3;4;5
2,myfile2.pdf,AAB,pdf,6;7
8,myfile8.pdf,AA8,pdf,99
```

### md5File
```yaml
# Generates the Md5 of all file paths found in the provided column in the csv
# This is a multi-threaded operation
- name: multi_threaded_file_md5
  operation: filesMd5
  keepState: true
  args:
    filenameCol: # name of the column holding the file path values
      value: filename
    md5Col: # name of the dynamic column we want to create from this operation
      value: md5
    outCols: # the columns we want out
      values: [id, filename, code, ext]
    threads: # number of threads to run concurrently
      value: 4
```

Example output:
```csv
id,filename,code,ext,md5
1,myfile1.pdf,AAA,pdf,pdf,39eb9fc737fc68b835b6e19b10f02de9
2,myfile2.pdf,AAB,pdf,pdf,fb09f3614bf7d7b733cb7dfd2cb580c5
8,myfile8.pdf,AA8,pdf,pdf,71576f06694d961f32a10b8cb7520bd7
```

### mergeDupes
```yaml
- name: merging_all_duplicates
  operation: mergeDupes
  fromState: multi_threaded_file_md5 # using the output of the multi_threaded_file_md5 operation
  args:
    indexCols: # list of columns used for comparison
      values: [code]
    outCols: # the columns we want out
      values: [id, filename, code, ext, md5]
    mergeValues: # If 'false' we keep the first row we find, else we keep the first non-empty column from the group of duplicate rows we found
      value: true
```

Example output:
```csv
id,filename,code,ext,md5
1,myfile1.pdf,AAA,pdf,pdf,39eb9fc737fc68b835b6e19b10f02de9
2,myfile2.pdf,AAB,pdf,pdf,fb09f3614bf7d7b733cb7dfd2cb580c5
8,myfile8.pdf,AA8,pdf,pdf,71576f06694d961f32a10b8cb7520bd7
```


### print
```yaml
# Prints the output of an operation to stdout
- name: printing_id
  operation: print
  FromState: merging_all_duplicates
  args:
    cols:
      values: [id, filename, code ext, md5]
```

### toFile
```yaml
# Writes the output of an operation to a file
- name: write_merge_output_to_file
  operation: toFile
  fromState: merging_all_duplicates
  args:
    filename:
      value: "/Users/me/Downloads/md5.csv"
    cols:
      values: [id, filename, code ext, md5]
```
