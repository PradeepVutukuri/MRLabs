register /home/train/workspace/Pig/stockudfs.jar;

stockdata = LOAD 'stocksA/NYSE_daily_prices_A.csv' using PigStorage(',') AS (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int);
stocks_all = FOREACH stockdata GENERATE symbol, date, close, volume;
stocks_filter = FILTER stocks_all BY symbol == '$symbol';
obv_result = FOREACH stocks_filter GENERATE symbol, date, stockudfs.OnBalanceVolume(volume, close) AS obv;
dump obv_result;
