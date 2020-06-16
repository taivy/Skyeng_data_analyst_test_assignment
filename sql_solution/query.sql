SELECT COUNT(*) FROM (
    WITH pages as (
    	SELECT *,
    	(case page when 'rooms.homework-showcase' then 1 when 'rooms.view.step.content' then 2 when 'rooms.lesson.rev.step.content' then 3 else 4 end) activity_number,
    	extract('epoch' from happened_at) - extract('epoch' from lag(happened_at) OVER (PARTITION BY user_id ORDER BY happened_at)) time_lag
    	FROM test.vimbox_pages
    	WHERE (page IN ('rooms.homework-showcase',
    	                  'rooms.view.step.content',
    	                  'rooms.lesson.rev.step.content'))
    	ORDER BY happened_at
    )
    
    select user_id, min(min_dt) min_dt, max(max_dt) max_dt, sess_start_sum
      from (
        select user_id, sess_start_sum, act_start_sum, min(happened_at) min_dt, max(happened_at) max_dt,
               count(distinct activity_number) act_cnt
          from (
            select *,
                   sum(sess_start) over(partition by user_id order by happened_at rows between 1 following and 1 following) sess_start_sum,
                   sum(act_start) over(partition by user_id order by happened_at rows between 1 following and 1 following) act_start_sum
               from (
                select *,
                       case when 
                         coalesce(extract('epoch' from happened_at-lag(happened_at) over(partition by user_id order by happened_at)),0) < 3600
                       then 0 else 1 end sess_start,
                       case when lag(activity_number) over(partition by user_id order by happened_at) > activity_number then 1 else 0 end act_start
                  from pages
               ) X
          ) Y
         group by user_id, sess_start_sum, act_start_sum
      ) Z 
      group by user_id, sess_start_sum
      having max(act_cnt)=3
)