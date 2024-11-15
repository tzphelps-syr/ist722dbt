with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_product as (
    select * from {{ ref('dim_product')}}
)
select 
    d_customer.*, 
    d_employee.*, 
    d_date.*,
    d_product.*,
    fs.orderid, fs.extendedpriceamount, fs.discountamount, fs.soldamount
    from f_sales as fs
    left join d_customer on fs.customerkey = d_customer.customerkey
    left join d_employee on fs.employeekey = d_employee.employeekey
    left join d_date on fs.orderdatekey = d_date.datekey
    left join d_product on fs.productkey = d_product.productkey
