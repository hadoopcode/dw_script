#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d "-1 day" +F%`
fi

sql="
use $APP;
load data inpath '/sys_origin_data/$APP/db/order_info/"$do_date"' overwrite into table ods_order_info partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/order_detail/"$do_date"' overwrite into table order_detail partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/sku_info/"$do_date"' overwrite into table sku_info partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/user_info/"$do_date"' overwrite into table user_info partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/payment_info/"$do_date"' overwrite into table payment_info partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/base_category1/"$do_date"' overwrite into table base_category1 partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/base_category2/"$do_date"' overwrite into table base_category2 partition (dt='$do_date');
load data inpath '/sys_origin_data/$APP/db/base_category3/"$do_date"' overwrite into table base_category3 partition (dt='$do_date');
"


$hive -e "$sql"
