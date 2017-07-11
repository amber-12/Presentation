--find customer id for the customer who has spent maximum amount

Retail = load '/home/hduser/Retail Data' using PigStorage(';') as (date,custid:long,agegrp,pin,category:long,prodid:long,quantity:long,cost:double,salescost:double);

--dump Retail;



sale = group Retail by custid;

--dump sale;


sales = foreach sale generate group as custid,SUM(Retail.salescost) as total_sales;

--dump sales;


ordered_sale = order sales by total_sales desc;


result = limit ordered_sale 1;


dump result;


