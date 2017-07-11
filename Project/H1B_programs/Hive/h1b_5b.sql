use h1b_project; 


insert overwrite directory '/h1b/problem5b/2011' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2011 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5b/2012' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2012 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5b/2013' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2013 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5b/2014' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2014 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5b/2015' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2015 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5b/2016' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2016 AND case_status ="CERTIFIED" group by job_title,year order by total desc limit 10;












