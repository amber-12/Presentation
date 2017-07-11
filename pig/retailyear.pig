year1 = load '/home/hduser/2000.txt' using PigStorage (',') as (cid:long,pname,jan:double,feb:double,mar:double,april:double,may:double,jun:double,july:double,august:double,september:double,octo:double,nov:double,dec:double);

year2 = load '/home/hduser/2001.txt' using PigStorage (',') as (cid:long,pname,jan:double,feb:double,mar:double,april:double,may:double,jun:double,july:double,august:double,september:double,octo:double,nov:double,dec:double);

year3 = load '/home/hduser/2002.txt' using PigStorage (',') as (cid:long,pname,jan:double,feb:double,mar:double,april:double,may:double,jun:double,july:double,august:double,september:double,octo:double,nov:double,dec:double);

--dump year1;

year1col1 = foreach year1 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);

--dump year1col1;
 
year2col2 = foreach year2 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);


year3col3 = foreach year3 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);


joined = join year1col1 by $0, year2col2 by $0, year3col3 by $0;

--dump joined;

originaltb = foreach joined generate $0, $1, $2, $5, $8;

--dump originaltb;


--describe originaltb;

growths = foreach originaltb generate $0,$1,ROUND_TO((($3-$2)*100/$2),2) as grow1,ROUND_TO((($4-$3)*100/$3),2) as grow2;


--dump growths;
avggrowth = foreach growths generate $0,$1,(($2+$3)/2) as average;


avgthan10 = filter avggrowth by $2>=10;

dump avgthan10;


--lessavg = filter avggrowth by $2 < 0;

--drop5avg = filter lessavg by $2 < -5;


dump drop5avg;


--store avggrowth  into '/home/hduser/Desktop/output' using PigStorage(',');

