use h1b_project; 


insert overwrite directory '/h1b/problem5a/2011' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2011 group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5a/2012' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2012 group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5a/2013' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2013 group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5a/2014' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2014 group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5a/2015' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2015 group by job_title,year order by total desc limit 10;

 insert overwrite directory '/h1b/problem5a/2016' row format delimited fields terminated by ',' stored as textfile select job_title,year,count(case_status) as total from h1b_final where year=2016 group by job_title,year order by total desc limit 10;












