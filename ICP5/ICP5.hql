
--Create the stock table with the column names as described
-- include two columns, market and stock symbol, as the partitions,
-- from a CSV.
CREATE TABLE IF NOT EXISTS stocks (
  date      STRING,
  open      FLOAT, 
  high      FLOAT,
  low       FLOAT,
  close     FLOAT,
  volume    INT,
  adj_close FLOAT
)
PARTITIONED BY (market STRING, symbol STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--Load data into the table separately in order to use the different CSVs
-- to create partitions based on which file is loaded.
LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/thinkbig-hive-tutorial/data/stocks/input/plain-text/NYSE/GE' 
OVERWRITE INTO  TABLE stocks PARTITION (market='NYSE',symbol='GE');

LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/thinkbig-hive-tutorial/data/stocks/input/plain-text/NYSE/IBM' 
OVERWRITE INTO  TABLE stocks PARTITION (market='NYSE',symbol='IBM');

LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/thinkbig-hive-tutorial/data/stocks/input/plain-text/NASDAQ/AAPL' 
OVERWRITE INTO  TABLE stocks PARTITION (market='NASDAQ',symbol='AAPL');

LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/thinkbig-hive-tutorial/data/stocks/input/plain-text/NASDAQ/INTC'
OVERWRITE INTO  TABLE stocks PARTITION (market='NASDAQ',symbol='INTC');

--Statistics: find the average open, high, low, close, adjusted close price, and the volume
-- for each symbol and market
SELECT market, symbol, avg(open), avg(high), avg(low), avg(close), avg(volume), avg(adj_close)
FROM stocks
GROUP BY market, symbol;

--Pattern: find the average open, high, low, close, adjusted close price, and the volume
-- for each symbol and market when the open price is 20% higher than the closing price
SELECT market,symbol,avg(open),avg(high),avg(low),avg(close),avg(volume),avg(adj_close)
FROM stocks
WHERE open>close*1.2
GROUP BY market,symbol;

--Create a table and load the shakespeare data into it
CREATE TABLE IF NOT EXISTS shakespeare(words STRING);

LOAD DATA LOCAL INPATH "/home/cloudera/Desktop/thinkbig-hive-tutorial/data/shakespeare/input/" INTO TABLE shakespeare;

--Word count: count the occurence of each word in the Shakespeare data
-- Count each word from a subquery which uses regular expressions to 
-- pull out each separate word (\W+), the extra \ is to escape the following
-- \ in \W+. Each split word is then segregated into its own cell, which is then,
-- subsequently counted. The results are ordered by occurence and limited to the top 20.
SELECT word,count(word) as count
FROM (SELECT explode(split(words, '\\W+')) as word FROM shakespeare) w
GROUP BY word
ORDER BY count desc
LIMIT 20;