## Issue filter

## Example:

```
# config.json

{
  "newspapers": {
      "CDV": ["1840/01/01", "1973/01/01", "1973/10/29"],
      "GDL": "1840/04/02-1840/04/02",
      "JDG": []
    },
  "exclude_newspapers": ["02_GDL", "TEST", "EXP"]
  "year_only": false
}
```

## Comments:

About positive/negative filtering:   

- it is mandatory to define both `newspapers:{}` and `exclude_newspapers:[]` members.
- if `exclude_newspapers` is empty, the `newspapers` positive filtering applies.
- if `exclude_newspapers` is not empty, the negative filtering applies. `newspapers` should still be present, and can be empty or not.


About newspapers object:   

- selected issues will include the specified newspapers
- for each newspaper, the specified dates will be included
- date format should always be `yyy/mm/dd`, even when `year_only=true`
- possible date specifications:
	1. a list of dates (array of strings)
	2. a date range (string with hyphen)
	3. an empty list => not date filter, takes all issues of that newspaper

About year flag:

For (1) and (2) above, `year_only` indicate whether to consider the exact date or the year only. 

