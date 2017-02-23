STREAM="hadoop jar  /usr/local/Cellar/hadoop/2.7.3/libexec/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -conf conf/hadoop-localhost.xml"

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files max_daily_temp_map.py,\
max_daily_temp_reduce.py \
  -input input/ncdc/all \
  -output out_max_daily \
  -mapper max_daily_temp_map.py \
  -reducer max_daily_temp_reduce.py

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files mean_max_daily_temp_map.py,\
mean_max_daily_temp_map.py \
  -input out_max_daily \
  -output out_mean_max_daily \
  -mapper mean_max_daily_temp_map.py \
  -reducer mean_max_daily_temp_reduce.py

  
