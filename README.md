# Luigi examples

A simple Luigi job demo at Python Stuttgart Meetup.


# Try

1. Run a task

`python -m luigi --module example SalesReport --date=2019-04-26`

2. Process date range

`python -m luigi --module example RangeDaily --of SalesReport --start=2019-04-22`
