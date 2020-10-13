# Data Processing


## How to run 

### Import weather data



### Import state codes
1. Find the file `state_code.csv` in S3 and edit the file location in the script.
1. Submit `import_state_codes.py` to Spark (no parameters) with `submit-import-states.sh`

### Import mortality data
1. Add files in S3 and edit the file location in the script.
1. Edit the position descriptor file named `mort_schema.json` to reflect the data files in S3 in this folder.
1. Make sure the `state_codes.csv` is available in the folder.
1. Submit `append_mortality_data.py` to Spark with the first and last year as parameters (e.g. 1985 1992)
with `submit-append-mort.sh`.





### Incremental addition
1. Add file in S3 and add corresponding field position in `mort_schema.json`.
1. Submit `append_mortality_data.py` to Spark with the added years as parameters (e.g. 1993 1997).


