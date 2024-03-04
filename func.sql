--To continue

-- calculate min, max coordinates
-- start
DROP FUNCTION min_max_coordinates();

CREATE OR REPLACE FUNCTION min_max_coordinates()
RETURNS TABLE (oid INT, minx double precision, miny double precision, minz double precision, maxx double precision, maxy double precision, maxz double precision) AS $$
BEGIN
    RETURN QUERY
	
select 
id,
min(x) as minx, min(y) as miny, min(z) as minz, 
max(x) as maxx, max(y) as maxy, max(z) as maxz
from(
select 
	unnest(nodes[1:][1]) as x, unnest(nodes[1:][2]) as y, unnest(nodes[1:][3]) as z, id 
	--, nodes[1:2][1:] as node
	from object as t1
) as t2
group by id;

END $$ LANGUAGE plpgsql;

SELECT * FROM min_max_coordinates();
-- end