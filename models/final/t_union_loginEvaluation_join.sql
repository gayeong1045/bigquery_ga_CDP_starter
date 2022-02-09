-- t_union_all과 loginhistory_evaluation의 left join
select * 
from {{ ref('t_union_all') }} a
    left join {{ ref('loginhistory_evaluation') }} b
    on a.userID = b.userID_account