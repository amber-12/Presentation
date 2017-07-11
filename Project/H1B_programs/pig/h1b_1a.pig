h1bvisa = load '/home/hduser/h1b_final/' using PigStorage() as (sr_no:chararray,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,work_site:chararray,longitude:chararray,latitude:chararray);

data = foreach h1bvisa generate $4,$7;

dengg= filter data by job_title matches'.*DATA ENGINEER.*';

--dump data;.

year11 = filter dengg by year == '2011';
year12 = filter dengg by year == '2012';
year13 = filter dengg by year == '2013';
year14 = filter dengg by year == '2014';
year15 = filter dengg by year == '2015';
year16 = filter dengg by year == '2016';

data11 = group year11 by year;

total11 = foreach data11 generate group as year,COUNT(year11.job_title);
--dump total11;


data12 = group year12 by year;
total12 = foreach data12 generate group as year,COUNT(year12.job_title);

data13 = group year13 by year;
total13 = foreach data13 generate group as year,COUNT(year13.job_title);

data14 = group year14 by year;
total14 = foreach data14 generate group as year,COUNT(year14.job_title);

data15 = group year15 by year;
total15 = foreach data15 generate group as year,COUNT(year15.job_title);



data16 = group year16 by year;
total16 = foreach data16 generate group as year,COUNT(year16.job_title);

result = UNION total11,total12,total13,total14,total15,total16;

final = order result by year asc;

--RESULT WILL BE SHOWN AS YEAR,TOTAL APPLICATION OF DATAENGG 
store final into '/home/hduser/project/output1a' using PigStorage();
