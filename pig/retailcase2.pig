-- top 10 product being sold in monthly basis

Retail = load '/home/hduser/Retail Data' using PigStorage(';') as (date,custid:long,agegrp,pin,category:long,prodid:long,quantity:long,cost:double,salescost:double);


groupbyproduct = group Retail by prodid;

--dump groupbyproduct;

totalsale = foreach groupbyproduct generate group as product_id, SUM(Retail.salescost) as total_sales;

--dump totalsale;


desc_ordered = order totalsale by total_sales desc;


result = limit desc_ordered 10;

dump result;
 
