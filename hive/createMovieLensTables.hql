SET hive.support.sql11.reserved.keywords=false;
CREATE TABLE IF NOT EXISTS movies ( movieId int, title String, genres String)
COMMENT 'Movies details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS tags ( userId int, movieId int, tag String, timestamp bigint)
COMMENT "Movie Tags"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ratings ( userId int, movieId int, rating double, timestamp bigint)
COMMENT "Movie Ratings"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\054'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

