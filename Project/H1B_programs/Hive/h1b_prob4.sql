use h1b_project; 

insert overwrite directory '/h1b/problem4/2015' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2015 group by employer_name,year order by total desc limit 5;


 insert overwrite directory '/h1b/problem4/2011' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2011 group by employer_name,year order by total desc limit 5;


insert overwrite directory '/h1b/problem4/2012' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2012 group by employer_name,year order by total desc limit 5;


insert overwrite directory '/h1b/problem4/2013' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2013 group by employer_name,year order by total desc limit 5;

 insert overwrite directory '/h1b/problem4/2014' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2014 group by employer_name,year order by total desc limit 5;


insert overwrite directory '/h1b/problem4/2016' row format delimited fields terminated by ',' stored as textfile select employer_name,year,count(case_status) as total from h1b_final where year=2016 group by employer_name,year order by total desc limit 5;
