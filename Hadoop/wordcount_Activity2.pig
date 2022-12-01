-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/root/file01.txt' AS (line);
-- Tokenize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grouped = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grouped GENERATE group, COUNT(words);
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/root/results';
