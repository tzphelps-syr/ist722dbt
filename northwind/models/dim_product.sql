with stg_products as (
    select * from {{ source('northwind','Products')}}
),
stg_categories as (
    select * from {{ source('northwind','Categories')}}
)
select  {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as productkey, 
    p.productid, p.productname, p.supplierid as supplierkey,
    c.categoryname, c.description
from stg_products p
    LEFT JOIN stg_categories c on p.categoryid = c.categoryid