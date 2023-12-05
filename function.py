def load_data(vehicle_id, date_str):
    start_time = time.time()
    date_object = datetime.strptime(date_str, '%Y-%m-%d')

    # Query to fetch the main parameters from iqube_aggregated
    query1 = f"SELECT vehicle, trip_start_time,trip_end_time,trip_start_lon,trip_start_lat,trip_end_lon,trip_end_lat,city,max_motor_temp,max_ecu_temp,brake_tps,tps_80_100,tps_60_80,speed_0_20,speed_40_60,eco_duration,pow_duration,trip_distance,eco_distance,pow_distance,overall_range from iqube_aggregated WHERE vehicle = {vehicle_id} ORDER BY trip_start_time DESC LIMIT 10;"

    # Fetch df1
    df1 = pd.read_sql_query(sql=query1, con=conn)

    # Define an empty DataFrame to store the results of df2 and df3
    df2_results = pd.DataFrame()
    df3_results = pd.DataFrame()

    # Iterate through each row of df1
    for index, row in df1.iterrows():
        # Extract trip_start_time and trip_end_time
        st = row['trip_start_time']
        et = row['trip_end_time']
        d = row["trip_distance"]*1000

        # Query to fetch additional parameters from iqubeug_raw
        query2 = f"""
            SELECT
                vehicle,
                CAST(stddevSamp(ecu_temp) AS FLOAT) AS ecu_temp_std_dev,
                CAST(stddevSamp(motor_temp) AS FLOAT) AS motor_temp_std_dev,
                CAST(stddevSamp(tps_per) AS FLOAT) AS tps_std_dev,
                CAST(
                    stddevSamp(
                        CASE
                            WHEN Bat_A_Max_cell_Temp <= 75 AND Bat_A_Max_cell_Temp >= 15 AND 
                                 Bat_B_Max_cell_Temp <= 75 AND Bat_B_Max_cell_Temp >= 15 AND 
                                 Bat_B_Max_cell_Temp >= Bat_A_Max_cell_Temp 
                            THEN Bat_B_Max_cell_Temp
                            WHEN Bat_A_Max_cell_Temp <= 75 AND Bat_A_Max_cell_Temp >= 15 AND 
                                 Bat_B_Max_cell_Temp <= 75 AND Bat_B_Max_cell_Temp >= 15 AND 
                                 Bat_B_Max_cell_Temp < Bat_A_Max_cell_Temp 
                            THEN Bat_A_Max_cell_Temp
                            ELSE 0
                        END
                    ) AS FLOAT
                ) AS max_cell_temp_std_dev
            FROM iqube.iqubeug_raw
            WHERE time >= '{st}' AND time <= '{et}' AND vehicle = {vehicle_id}
            GROUP BY vehicle
        """
        
        
        df2 = pd.read_sql_query(sql=query2, con=conn)
        df2_results = df2_results.append(df2, ignore_index=True)

        # Query to fetch parameters from iqubeug_raw using query3
        query3 = f"""
            SELECT
        vehicle,
        CAST((SUM(CASE WHEN acc > 1.8 THEN 1 ELSE 0 END) / {d}) AS FLOAT) AS ha_p_m,
        CAST((SUM(CASE WHEN acc < -2.78 THEN 1 ELSE 0 END) / {d}) AS FLOAT) AS hb_p_m,
        CAST((SUM(brake) / {d}) AS FLOAT) AS b_p_m,
        CAST(AVG(jerk) AS FLOAT) AS jerk_mean
    FROM (
        SELECT
            t1.vehicle,
            ABS(((CAST(t1.speed AS DOUBLE) - CAST(t2.speed AS DOUBLE)) / (CAST(t1.time AS DOUBLE) - CAST(t2.time AS DOUBLE)))) AS jerk,
            CASE WHEN t1.speed > 1.8 THEN 1 ELSE 0 END AS acc,
            t1.brake,
            t1.time
        FROM
            (SELECT vehicle, speed, brake, time FROM iqubeug_raw WHERE time >= '{st}' AND time <= '{et}' AND vehicle = {vehicle_id}) t1
        INNER JOIN
            (SELECT vehicle, speed, time FROM iqubeug_raw WHERE time >= '{st}' AND time <= '{et}' AND vehicle = {vehicle_id}) t2
        ON
            t1.time = t2.time + 1
    )
    GROUP BY vehicle


        """
        
        # Fetch df3
        df3 = pd.read_sql_query(sql=query3, con=conn)
        df3_results = df3_results.append(df3, ignore_index=True)
    

    # Merge df2_results and df3 on the vehicle column
    calculated_df = pd.concat([df2_results, df3_results],axis=1)
        
    final_df=pd.concat([df1,calculated_df], axis=1)
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time} seconds")

    return final_df
