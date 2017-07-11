h1b = load '/h1b_final' using PigStorage() as(sno:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray,full_time_position:chararray,prevailining_wage:double, year:chararray, worksite:chararray, longitude:int, latitude:int);


data = foreach h1b generate year,sno;

yeardata = group data by year;

total = foreach yeardata generate group as year,COUNT(data.sno) as application;


result = order total by year desc;

store result into '/h1b/output7' using PigStorage();

