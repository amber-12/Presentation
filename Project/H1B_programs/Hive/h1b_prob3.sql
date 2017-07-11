use h1b_project; 

insert overwrite directory '/h1b/problem3' row format delimited fields terminated by ',' stored as textfile select soc_name,count(job_title) as total from h1b_final where job_title ="DATA SCIENTIST" group by soc_name order by total desc limit 10;

