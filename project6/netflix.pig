loadData = LOAD '$G' USING PigStorage(',') AS (x_value: int, y_value: double);
filterData = FILTER loadData BY (x_value IS NOT NULL);
groupData  = GROUP filterData BY x_value;

UserRating = FOREACH groupData GENERATE group, AVG(filterData.y_value) as sorted_value;
group_value = GROUP UserRating BY FLOOR(sorted_value*10)/10;
outputFile = FOREACH group_value GENERATE group, COUNT(UserRating);

STORE outputFile INTO '$O' USING PigStorage (' ');
