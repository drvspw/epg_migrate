-- First query
CREATE TABLE IF NOT EXISTS test(
  id SERIAL PRIMARY KEY,
  name VARCHAR(255)
);

--+ begin a function
CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
        BEGIN
                RETURN i + 1;
        END;
$$ LANGUAGE plpgsql;
--+ end a function

-- second query
CREATE TABLE IF NOT EXISTS ids(id SERIAL PRIMARY KEY);
