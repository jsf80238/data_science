# Jupyter
[Notebooks](notebooks)

# profile_data.py
## Overview
The idea for this code came from my time managing a team of data analysts for an internal audit department of a large company.

The audit team asks the business for certain data and, depending on the prevailing politics of the organization, may eventually receive such data or the permission necessary to extract it.

So, now you've got potentially a lot of data ... where do you tell your auditors to focus? Wouldn't it be helpful to have a program which quickly identifies interesting things? And by interesting I mean either likely incorrect, or likely correct but pointing to a business failure.

That's what this program does.

## Usage
I grabbed sample Los Angeles restaurant inspection data from https://www.kaggle.com.
The URL at that time was https://www.kaggle.com/datasets/cityofLA/la-restaurant-market-health-data. 

### Installation
- `git clone https://github.com/jsf80238/data_science.git`
- `cd data_profiling`
- `python3 -m venv your_dir`
- `source your_dir/bin/activate`  # or on Windows `your_dir\Scripts\activate.bat`
- `pip install -r requirements.txt`

### Execution
    $ python data_science/profile_data.py -h
    usage: profile_data.py [-h] [--header-lines NUM] [--delimiter CHAR]
                           [--sample-rows-file NUM] [--max-detail-values NUM]
                           [--max-pattern-length NUM] [--max-longest-string NUM]
                           [--target-dir /path/to/dir] [--html] [--db-host-name HOST_NAME]
                           [--db-port-number PORT_NUMBER] [--db-name DATABASE_NAME]
                           [--db-user-name USER_NAME] [--db-password PASSWORD]
                           [--environment-file /path/to/file] [--verbose | --terse]
                           /path/to/input_data_file.csv | query-against-database
    
    Profile the data in a database or CSV file. Generates an analysis consisting tables and
    images stored in an Excel workbook or HTML pages. For string columns provides a pattern
    analysis with C replacing letters, 9 replacing numbers, underscore replacing spaces, and
    question mark replacing everything else. For numeric and datetime columns produces a
    histogram and box plots.
    
    positional arguments:
      /path/to/input_data_file.csv | query-against-database
                            An example query is 'select a, b, c from t where x>7'.
    
    options:
      -h, --help            show this help message and exit
      --header-lines NUM    When reading from a file specifies the number of rows to skip UNTIL
                            the header row. Ignored when getting data from a database. Default
                            is 0. (must be in range 1..=9223372036854775807)
      --delimiter CHAR      Use this character to delimit columns, default is a comma. Ignored
                            when getting data from a database.
      --sample-rows-file NUM
                            When reading from a file randomly choose this number of rows. If
                            greater than or equal to the number of data rows will use all rows.
                            Ignored when getting data from a database. (must be in range
                            1..=9223372036854775807)
      --max-detail-values NUM
                            Produce this many of the top/bottom value occurrences, default is
                            35. (must be in range 1..=9223372036854775807)
      --max-pattern-length NUM
                            When segregating strings into patterns leave untouched strings of
                            length greater than this, default is 50. (must be in range
                            1..=9223372036854775807)
      --plot-values-limit NUM
                            Don't make histograms or box plots when there are fewer than this
                            number of distinct values, and don't make pie charts when there are
                            more than this number of distinct values, default is 8. (must be in
                            range 1..=9223372036854775807)
      --max-longest-string NUM
                            When displaying long strings show a summary if string exceeds this
                            length, default is 50. (must be in range 50..=9223372036854775807)
      --target-dir /path/to/dir
                            Default is the current directory. Will make intermediate
                            directories as necessary.
      --html                Also produce a zip file containing the results in HTML format.
      --db-host-name HOST_NAME
                            Overrides HOST_NAME environment variable. Ignored when getting data
                            from a file.
      --db-port-number PORT_NUMBER
                            Overrides PORT_NUMBER environment variable. Ignored when getting
                            data from a file.
      --db-name DATABASE_NAME
                            Overrides DATABASE_NAME environment variable. Ignored when getting
                            data from a file.
      --db-user-name USER_NAME
                            Overrides USER_NAME environment variable. Ignored when getting data
                            from a file.
      --db-password PASSWORD
                            Overrides PASSWORD environment variable. Ignored when getting data
                            from a file.
      --environment-file /path/to/file
                            An additional source of database connection information. Overrides
                            environment settings.
      --verbose
      --terse

- Download your data.
- `data_science/python analyze-quality.py ~/Downloads/restaurant-and-market-health-inspections.csv`
- View the results from `analysis.xlsx` in your current directory, or the `--target-dir` directory if provided.

### Results
The program generates a XLSX file containing multiple sheets:
- Summary.
- Number-of-occurrences detail, one sheet per column in the data source.
- String patterns, one sheet per string-type column in the data source.
- Depending on the data and the `--plot-values-limit` argument:
  - Histogram, one sheet per numeric/datetime column in the data source.
  - Box plot, one sheet per numeric/datetime column in the data source.
  - Pie plot, for each column in the data source, but see

> [!TIP]
> Optionally, use the `--html` command-line argument to also generate `analysis.zip`.
> 
> Unzip and point your browser at `analysis.html`.

This is an example summary:
![Summary](images/summary.png)
Let's focus on the highlighted cells.
- C6, C19: these are likely data quality issues. As a percentage of the total data set can be ignored.
- F2: `serial_number` is unique. Good.
- G4: The most common `facility_name` for restaurants is "DODGER_STADIUM". That's odd.
- G16: And yet the most common `owner_name` is Ralph's Grocery CO. Probably https://www.ralphs.com/.
- L4: The shortest `facility_name` is "ZO". Probably a data quality issue.
- M3, Q3: Dates are treated as numeric. They can essentially be thought of as the number of seconds after some date. See also https://www.epochconverter.com/ for Linux. Windows has a [similar concept](https://devblogs.microsoft.com/oldnewthing/20090306-00/?p=18913). 
- N5, O5, P5: 50% of the scores were between 91 and 96.
- M7, M18: the program treats numbers as measurements, even though for these columns the numbers are just IDs. Perhaps more sophisticated code could do better.

Now, details by column.
#### score

![score.distribution](images/score.distribution.png)

- As a first estimate I would have guessed this would look like a Bell curve, perhaps with a bit of [skew](https://www.itl.nist.gov/div898/handbook/eda/section3/eda35b.htm).
- Instead, we many more scores of 90 than expected and much fewer scores of 89 than expected (and fewer in the 80s than expected).
- Without proof I would guess:
  - A score of 90-100 yields a sign in the restaurant window with the letter A.
  - A score of 80-99 yields a sign in the restaurant window with the letter B.
  - People don't like to eat at restaurants which fail to achieve a A-rating.
  - Restaurant owners, and to a lesser extent restaurant inspectors, strive to avoid anything other than a A-rating. (Image below courtesy of https://la.eater.com/2015/8/19/9178907/la-county-health-department-restaurant-grades-need-overhaul.)

![restaurant_rating_in_window](images/restaurant_rating_in_window.png)

#### employee_id

![employee_id.categorical](images/employee_id.categorical.png)

![employee_id_detail](images/employee_id_detail.png)

- One employee (EE0000721) among the 143 who performed inspections handled one out of every fourteen inspections. And it was twice as many as the next busiest inspector. Why?

#### activity_date

![activity_date_detail](images/activity_date_detail.png)

- Note the dates with very few inspections (F2, F3, F4 ...). These are Saturdays and Sundays. It makes sense inspectors (city staff) don't work as much on weekends.

#### facility_name

![facility_name_detail](images/facility_name_detail.png)

- Again, "DODGER STADIUM" leads the way. Are there more restaurants in Dodger Stadium than there are Subway restaurants in all of Los Angeles?

#### owner_name

![owner_name_detail](images/owner_name_detail.png)

- Note the yellow-highlighted cells. This looks to be a data-quality issue .. Levy Premium Food listed twice. When added together this would be the top owner, not Ralph's.
- Note the blue-highlighted cells. Is true there are only 50% more Starbucks than Whole Foods?

#### service_description

![service_description_detail](images/service_description_detail.png)

- Only 1.65% of inspections were initiated by the owner. Probably makes sense.
- All inspections are some variation of "routine", apparently.

## Potential improvements
- Allow the caller to specify unusual, but known, datetime formats.
- Allow the caller to specify columns to exclude, or include.
- Generate better plots. It is difficult to generate useful plots on an automated basis.
  - You might want a histogram for numeric or datetime data, but if the column is a primary key, or a created timestamp generated by a trigger, then each value will appear (almost always) one time, making a histogram uninteresting.
  - Allow the caller to specify plot visual effects.