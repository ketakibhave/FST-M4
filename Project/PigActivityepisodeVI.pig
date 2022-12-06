--Load data from HDFS
inputDialogs = LOAD 'hdfs:///user/root/inputs/episodeVI_dialouges.txt' USING PigStorage '\t' AS (name:chararray, line:chararray);
--Filter out header lines
ranked = RANK inputDialogs;
OnlyDialouges= FILTER ranked BY (rank_inputDialouges > 2);
-- Group by character names
grouped = GROUP OnlyDialouges BY name;
-- Count the number of dialogues
names = FOREACH grouped GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered= ORDER names BY no_of_lines DESC;
-- Store the result in HDFS
STORE namesOrdered INTO 'hdfs:///user/root/outputs/results_episodeVI';
