STREAM="hadoop jar  /usr/local/Cellar/hadoop/2.7.3/libexec/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -conf conf/hadoop-localhost.xml"

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files MaxMapper.py,\
MaxReducer.py \
  -input input/ncdc/all \
  -output out_max_daily \
  -mapper MaxMapper.py \
  -reducer MaxReducer.py

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files MeanMapper.py,\
MeanReducer.py \
  -input out_max_daily \
  -output out_mean_max_daily \
  -mapper MeanMapper.py \
  -reducer MeanReducer.py

  
