# **尚未修復的Bug**


1. dags/mydags/nodes/process_waveform - Function - get_waveform_time_windows : 
    
    `time_window.station = pick.inventory.station` (Line67)
    
    TimeWindow datclass 沒有 station，整行刪掉即可
   
    
2. dags/mydags/nodes/process_waveform - Function -get_instances
    
    `id = '.'.join([timewindow.starttime.isoformat(), stream[0].stats.network, stream[0].stats.station])`(Line 192) 
    
    id 不能用 timewindow 的 starttime (不同事件的timewindow starttime 可能因為 trace_length 給的小就會重複)
   
    
# **開發階段重要須知**
- data_processing 檢查是否使用平行化加速

- generate_dataset
    
    `get_events_list` `events_raw`只先使用部分events
    
- Airflow docker_compose.yaml 有開放權限之後要注意防火牆安全性 (user: root)

- station_time_window 沒有辦法有效解決重複 station 的問題

- 未來可以加入辨識 picktime 來自哪個channel ，也就是模型輸出三條channel給的picktime，難點是訓練的時候人工label也要有對應的資訊
