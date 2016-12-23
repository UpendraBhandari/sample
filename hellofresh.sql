create view latest_ingredients as
select a.id_ingredient, a.ingredient_name, a.price, a.timestamp1 as "timestamp", a.deleted
from (select *,rank() over (partition by ingredient_name order by timestamp1 desc,deleted asc) as valid from ingredient where deleted=0) a where a.valid=1;
