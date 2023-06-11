select 
 sum("Boyd_Yards") as "Boyd Yards"
,sum("Higgins_Yards") as "Higgins Yards"
,sum("Chase_Yards") as "Chase Yards"
,concat(
	sum(case when "Result" = 'Win' then 1 else 0 end),
	'-',
	sum(case when "Result" = 'Loss' then 1 else 0 end)
	) as "Win/Loss"
from "Anthony_Barone" ab 