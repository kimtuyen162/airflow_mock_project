DROP TABLE IF EXISTS {{ params.table_name }};

CREATE TABLE {{ params.table_name }}(
    category                TEXT, 
    sub_category            TEXT, 
    aggregation_date        DATE, 
    millions_of_dollar      FLOAT, 
    pipeline_exc_datetime   TIMESTAMP
);

COPY {{ params.table_name }}(category, sub_category, aggregation_date, millions_of_dollar,pipeline_exc_datetime)
FROM '{{ params.filepath }}'
DELIMITER ','
CSV HEADER;