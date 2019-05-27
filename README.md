### edbfly
`escript` application for postgresql migrations

### Prerequisites
The following table should exist in the database

```sql
CREATE TABLE IF NOT EXISTS migrations(
       name VARCHAR(255) PRIMARY KEY,
       status VARCHAR(255),
       time TIMESTAMPTZ
);
```

### Build
1. Clone this repository

2. Create executable script
```bash
$ make compile
$ make escriptize
```

This creates `edbfly` escript in `_build/default/bin` directory

### Usage
