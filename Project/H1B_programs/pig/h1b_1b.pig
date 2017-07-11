h1bvisa = load '/home/hduser/h1b_final/' using PigStorage() as (sr_no:chararray,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,work_site:chararray,longitude:chararray,latitude:chararray);

data = foreach h1bvisa generate $4,$7;

year11 = filter data by year == '2011';
year12 = filter  data by year == '2012';
year13 = filter data by year == '2013';
year14 = filter  data by year == '2014';
year15 = filter  data by year == '2015';
year16 = filter  data by year == '2016';

data11 = group year11 by job_title;

total11 = foreach data11 generate group as job_title,COUNT(year11.$1)as no;
--dump total11;


data12 = group year12 by job_title;
total12 = foreach data12 generate group as job_title,COUNT(year12.$1)as no;

data13 = group year13 by job_title;
total13 = foreach data13 generate group as job_title,COUNT(year13.$1) as no;

data14 = group year14 by job_title;
total14 = foreach data14 generate group as job_title,COUNT(year14.$1)as no;

data15 = group year15 by job_title;
total15 = foreach data15 generate group as job_title,COUNT(year15.$1)as no;

data16 = group year16 by job_title;
total16 = foreach data16 generate group as job_title,COUNT(year16.$1)as no;



join_data1 = join total11 by job_title ,total12 by job_title;

join_data2 = join total12 by job_title ,total13 by job_title;


join_data3 = join total13 by job_title ,total14 by job_title;

join_data4 = join total14 by job_title ,total15 by job_title;

join_data5 = join total15 by job_title ,total16 by job_title;


filterdata1 = foreach join_data1 generate $0,$1,$3;
growth1 = foreach filterdata1 generate $0,(double)($2-$1)*100/$1 as grwoth11;


filterdata2 = foreach join_data2 generate $0,$1,$3;
growth2 = foreach filterdata2 generate $0,(double)($2-$1)*100/$1 as grwoth12;

filterdata3 = foreach join_data3 generate $0,$1,$3;
growth3 = foreach filterdata3 generate $0,(double)($2-$1)*100/$1 as grwoth13;


filterdata4 = foreach join_data4 generate $0,$1,$3;
growth4 = foreach filterdata4 generate $0,(double)($2-$1)*100/$1 as grwoth14;


filterdata5 = foreach join_data5 generate $0,$1,$3;
growth5 = foreach filterdata5 generate $0,(double)($2-$1)*100/$1 as grwoth15;


growth = join growth1 by $0,growth2 by $0,growth3 by $0,growth4 by $0,growth5 by $0;

maindata = foreach growth generate $0,$1,$3,$5,$7,$9;

avggrowth = foreach maindata generate $0,(double)(($1+$2+$3+$4+$5)/5) as average;

final = order avggrowth by $1 desc;

result = limit final 5;

store result into '/home/hduser/project/h1b_1b' using PigStorage();











