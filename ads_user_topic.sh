#!/bin/bash
APP=gmall
hive=/opt/module/hive/bin/hive
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
do_date=$1
else
do_date=`date -d "-1 day" +%F`
fi
sql="
with
tmp_day_users as
(
select
'$do_date' dt,
count(*) day_users
from
${APP}.dwt_user_topic
where
login_date_last='$do_date'
),
tmp_day_new_users as
(
select
'$do_date' dt,
count(*) day_new_users
from
${APP}.dwt_user_topic
where
login_date_last='$do_date' and login_date_first='$do_date'
),
tmp_day_new_payment_users as
(
select
'$do_date' dt,
count(*) day_new_payment_users
from
${APP}.dwt_user_topic
where
payment_date_first='$do_date'
),
tmp_payment_users as
(
select
'$do_date' dt,
count(*) payment_users
from
${APP}.dwt_user_topic
where
payment_date_first is not null
),
tmp_users as
(
select
'$do_date' dt,
count(*) users
from
${APP}.dwt_user_topic
tmp_users
)
insert into table ${APP}.ads_user_topic
select
'$do_date' dt,
day_users,
day_new_users,
day_new_payment_users,
payment_users,
users,
day_users/users,
payment_users/users,
day_new_users/users
from
tmp_day_users
join
tmp_day_new_users
on
tmp_day_users.dt=tmp_day_new_users.dt
join
tmp_day_new_payment_users
on
tmp_day_users.dt=tmp_day_new_payment_users.dt
join
tmp_payment_users
on
tmp_day_users.dt=tmp_payment_users.dt
join
tmp_users
on
tmp_day_users.dt=tmp_users.dt;
"
$hive -e "$sql"