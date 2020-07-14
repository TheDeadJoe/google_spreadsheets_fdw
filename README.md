# Google Spreadsheets FDW

Multicorn based PostgreSQL foreign data wrapper for Google Spreadsheets

## Installation

### Requirements

PostgreSQL 9.1+ with [Multicorn](http://multicorn.org/) extension installed.

If you haven't used Multicorn yet, enable it with:

```postgresql
create extension multicorn;
```

From source:

```bash
git clone https://github.com/TheDeadJoe/google_spreadsheets_fdw
cd google_spreadsheets_fdw
python setup.py install
```

## Usage

```postgresql
create server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'google_spreadsheets_fdw.GoogleSpreadsheetFDW'
);

create foreign table my_table (
    id int default nextval('my_seq'),
    foo varchar,
    bar int,
    baz float
) server multicorn_srv options (
    gskey 'zVshdGDuaQKKaQoXqNOwjeTWcxcUtOlSJDZoLeIMUsYx',
    keyfile '/path/to/credentials.json',
    sheet '0',
    row_id 'id'
);
```

### Options

- `gskey` - "ID" of a spreadsheet (the 44 char length part between `/spreadsheets/d/` and `/edit/` from spreadsheet URL)
- `keyfile` - path to Google Cloud Services credentials json file
- `sheet` - index of a sheet
- `row_id` - name of the column which value will be treated as an ID of the whole row

## Example

We start with an empty spreadsheet:

![1](https://user-images.githubusercontent.com/8329442/87254938-d9629600-c486-11ea-8ed2-b5ccb342868b.png)

Let's insert some data into it:

```postgresql
insert into my_table(foo, bar, baz) values ('a', 1, 0.1);
insert into my_table(foo, bar, baz) values ('b', 2, 0.2);
insert into my_table(foo, bar, baz) values ('c', 3, 0.3);
insert into my_table(baz, bar, foo) values (0.4, 4, 'd') returning *;
```

Spreadsheet contains our data.

![2](https://user-images.githubusercontent.com/8329442/87254971-2a728a00-c487-11ea-88ac-9916b84af62f.png)

Now let's try retrieve the data:

```postgresql
select * from my_table;
```

The result:

![3](https://user-images.githubusercontent.com/8329442/87254972-2ba3b700-c487-11ea-8a44-4d993aeeeccd.png)


Of course, we can also perform other SQL operations e.g.: 

```postgresql
update my_table set bar = 9, baz = 0.9 where bar = 2;

delete from my_table where bar > 5;
```
