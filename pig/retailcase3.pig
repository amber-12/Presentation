-- top 10 product being sold in monthly basis

Retail = load '/home/hduser/Retail Data' using PigStorage(';') as (date,custid:long,agegrp,pin,category:long,prodid:long,quantity:long,cost:double,salescost:double);

groupbypndage = group Retail by ($2, $5) ;

--dump groupbypndage;

sale = foreach groupbypndage generate group as age1prod2,SUM(Retail.salescost) as totalsale;

--dump sale;

ordered_sale = order sale by totalsale desc;

--dump order_sale;


result = limit ordered_sale 5;

dump result;

