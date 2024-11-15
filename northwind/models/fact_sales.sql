with stg_orders as 
(
    select
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        Quantity,
        sum(Quantity*UnitPrice) as extendedpriceamount,
        sum((Quantity*UnitPrice)*Discount) as discountamount,  
        sum(Quantity*UnitPrice*(1-Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by orderid, productid, Quantity
)
select  
    o.OrderID, o.employeekey, o.customerkey, o.orderdatekey,
    od.Quantity, od.productkey, od.extendedpriceamount, od.discountamount,
    od.soldamount
from stg_orders o
    left join stg_order_details od on o.orderid = od.orderid
