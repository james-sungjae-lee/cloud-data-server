SET hive.support.sql11.reserved.keywords=false;
CREATE TABLE IF NOT EXISTS movies ( movieId int, title String, genres String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS tags ( userId int, movieId int, tag String, timestamp bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ratings ( userId int, movieId int, rating double, timestamp bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

