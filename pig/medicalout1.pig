medical_data = load '/home/hduser/medical.txt' using PigStorage() as (name,dept,claim:double);

groupbyuser = group medical_data by name;

--dump groupbyuser;

avgclaim = foreach groupbyuser generate group,ROUND_TO(AVG(medical_data.claim),2) as avguser;


dump avgclaim;
