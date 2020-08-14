#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d "-1 day"+%F`
fi

sql="
use $APP;
insert into table ads_uv_count
select '$do_date' dt,
       daycount.ct,
       wkcount.ct,
       mncount.dt,
       if(date_add(next_day('$do_date','MO'),-1)='$do_date','Y','N'),
       if(last_day('$do_date')='$do_date','Y','N')
from
(
    select '$do_date' dt,
           count(*) ct
    from dws_uv_detail_day
    where dt='$do_date'
    group by mid_id
) daycount join
(
    select '$do_date' dt,
           count(*) ct
    from dws_uv_detail_wk
    where wk_dt=concat(date_add(next_day('$do_date','MO'),-7),'_',date_add(next_day('$do_date','MO'),-1))
    group by mid_id
) wkcount on daycount.dt=wkcount.dt join
(
    select '$do_date' dt,
           count(*)
    from dws_uv_detail_mn
    where mn=date_format('$do_date','yyyy-MM')
    group by mid_id
) mncount on wkcount.dt=mncount.dt
"

$hive -e "$sql"