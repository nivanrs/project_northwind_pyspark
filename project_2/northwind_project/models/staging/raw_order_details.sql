select
    *      
from
    {{ source('raw','order_details') }}
