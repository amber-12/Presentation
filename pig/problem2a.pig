

h1bvisa = load '/user/hive/warehouse/h1b_project.db/h1b_final/' using PigStorage() as (sr_no:chararray,case_satus:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,work_site:chararray,longitude:chararray,latitude:chararray);


data = foreach h1bvisa generate $4,$7,$8;

--data: {job_title: chararray,year: chararray,work_site: chararray}


year11 =filter data by year=='2011';
year12 =filter data by year=='2012';
year13 =filter data by year=='2013';
year14 =filter data by year=='2014';
year15 =filter data by year=='2015';
year16 =filter data by year=='2016';

/*
now make logic to find which part of us has most data engineer jobs?

*/

data_engineer_jobs11 = FILTER year11 by job_title matches '.*DATA ENGINEER.*';
grouped_data11 = group data_engineer_jobs11 by work_site;
--describe grouped_data;
count_jobs11 = foreach grouped_data11 generate group as area,COUNT(data_engineer_jobs11) as  no_of_job;
final11 = order count_jobs11 by no_of_job desc;
store final11 into '/project/prob2a/2011' using PigStorage();



data_engineer_jobs12 = FILTER year12 by job_title matches '.*DATA ENGINEER.*';
grouped_data12 = group data_engineer_jobs12 by work_site;
count_jobs12 = foreach grouped_data12 generate group as area,COUNT(data_engineer_jobs12) as  no_of_job;
final12 = order count_jobs12 by no_of_job desc;
store final12 into '/project/prob2a/2012' using PigStorage();


data_engineer_jobs13 = FILTER year13 by job_title matches '.*DATA ENGINEER.*';
grouped_data13 = group data_engineer_jobs13 by work_site;
count_jobs13 = foreach grouped_data13 generate group as area,COUNT(data_engineer_jobs13) as  no_of_job;
final13 = order count_jobs13 by no_of_job desc;
store final13 into '/project/prob2a/2013' using PigStorage();

data_engineer_jobs14 = FILTER year14 by job_title matches '.*DATA ENGINEER.*';
grouped_data14 = group data_engineer_jobs14 by work_site;
count_jobs14 = foreach grouped_data14 generate group as area,COUNT(data_engineer_jobs14) as  no_of_job;
final14 = order count_jobs14 by no_of_job desc;
store final14 into '/project/prob2a/2014' using PigStorage();

data_engineer_jobs15 = FILTER year15 by job_title matches '.*DATA ENGINEER.*';
grouped_data15 = group data_engineer_jobs15 by work_site;
count_jobs15 = foreach grouped_data15 generate group as area,COUNT(data_engineer_jobs15) as  no_of_job;
final15 = order count_jobs15 by no_of_job desc;
store final15 into '/project/prob2a/2015' using PigStorage();

data_engineer_jobs16 = FILTER year16 by job_title matches '.*DATA ENGINEER.*';
grouped_data16 = group data_engineer_jobs16 by work_site;
count_jobs16 = foreach grouped_data16 generate group as area,COUNT(data_engineer_jobs16) as  no_of_job;
final16 = order count_jobs16 by no_of_job desc;
store final16 into '/project/prob2a/2016' using PigStorage();
