import json
import logging
from datetime import datetime

date = datetime.now().strftime("%m-%d-%y - %H_%M_%S ")

def calculate_jip(machine_name:str, as_of_week:int):

    template = """
            {
            "name": "template notebook",
            "defaultInterpreterGroup": "spark_jip",
            "paragraphs": [
                    {
                    "text": ""
                    }
                ]
            }
    """

    logging.debug("[script calculate] Load template json..")
    data = json.loads(template)
    logging.debug("[script calculate] Load template json success..")

    text = "%spark_jip.pyspark\nfrom pyspark.sql.window import Window\nfrom pyspark.sql.types  import StringType, DecimalType, IntegerType, FloatType, DoubleType\nfrom pyspark.sql.functions import col, current_timestamp, sum, when, lit, lag, coalesce,lower,lit, concat, expr, monotonically_increasing_id, row_number\nimport pandas as pd\nfrom pyspark.sql.functions import date_format\nfrom datetime import datetime, timedelta\n\nmachine_name = \"MACHINE_CODE_NAME_JIP\"\nas_of_week = AS_OF_WEEK_CODE_JIP\n\n#Read table from Oracle\nquery = f\"\"\"SELECT * FROM GGGG_JIP_CALENDAR WHERE MACHINE='{machine_name}' AND AS_OF_WEEK = '{as_of_week}' \"\"\"\nquery1 = f\"\"\"SELECT * FROM GGGG_JIP_OPB_SIMULATION WHERE (OUTPUT_HR > 0 AND MACHINE='{machine_name}') AND (AS_OF_WEEK = '{as_of_week}') AND (EXCLUDE_FLAG IS NULL OR EXCLUDE_FLAG ='N' ) \"\"\"\n\ndf_cal = spark.read.format('jdbc').option('url', 'jdbc:oracle:thin:@10.206.130.208:1541/GRF')\\\n    .option('user', 'APEX_GGGG')\\\n    .option('dbtable',f\"({query})\") \\\n    .option('password', 'apex2022')\\\n    .option('driver', 'oracle.jdbc.driver.OracleDriver')\\\n    .load()\ndf_opb_sim = spark.read.format('jdbc').option('url', 'jdbc:oracle:thin:@10.206.130.208:1541/GRF')\\\n    .option('user', 'APEX_GGGG')\\\n    .option('dbtable',f\"({query1})\") \\\n    .option('password', 'apex2022')\\\n    .option('driver', 'oracle.jdbc.driver.OracleDriver')\\\n    .load()\n    \n# Convert All column to lower\ndf_cal = df_cal.toDF(*[c.lower() for c in df_cal.columns])\ndf_opb_sim = df_opb_sim.toDF(*[c.lower() for c in df_opb_sim.columns])\n#Cast Integer \ndf_opb_sim = df_opb_sim \\\n            .withColumn(\"simulation_qty\", df_opb_sim[\"simulation_qty\"].cast(IntegerType()))\\\n            .withColumn(\"opb_dtl_id\", df_opb_sim[\"opb_dtl_id\"].cast(IntegerType())) \\\n            .withColumn(\"as_of_week\", df_opb_sim[\"as_of_week\"].cast(IntegerType())) \\\n            .withColumn(\"priority_number\", df_opb_sim[\"priority_number\"].cast(IntegerType()))\\\n            .withColumn(\"opb_simulation_id\", df_opb_sim[\"opb_simulation_id\"].cast(IntegerType()))\\\n            .withColumn(\"output_hr\",  df_opb_sim[\"output_hr\"].cast(DoubleType()))\n\n#Order by priority number ascending\ndf_opb_sim = df_opb_sim.orderBy(df_opb_sim[\"priority_number\"], ascending=True)\n\n#Create column hasil_jam\ndf_cal = df_cal.withColumn(\n    \"hasil_jam\",\n    when(col(\"shift\") == 3, 24 - col(\"schedule_dt\") - col(\"jumatan\") - col(\"trial\") - col(\"management_sd\") - col(\"no_load\"))\n    .when(col(\"shift\") == 2, 16)\n    .when(col(\"shift\") == 1, 8)\n    .when(col(\"shift\") == 0, 0)\n    .otherwise(0).cast(DoubleType())\n)\n\n# df_cal = df_cal.withColumn(\"hasil_jam\",df_opb_sim[\"hasil_jam\"].cast(DoubleType()))\n\n#Create column plan/hrs\ndf_opb_sim = df_opb_sim.withColumn(\"plan/hrs\", expr(\"simulation_qty / output_hr\"))\ndf_opb_sim = df_opb_sim.withColumn(\"plan/hrs\",  df_opb_sim[\"plan/hrs\"].cast(DoubleType()))\n\n#Filter and sort\ndf_opb_sim_filter = df_opb_sim\ndf_cal_filter = df_cal.sort(col(\"calendar_date\").asc())\ndf_opb_sim_filter = df_opb_sim_filter.sort(col(\"priority_number\").asc())\ndf_cal_filter = df_cal_filter.withColumn(\"calendar_date\", date_format(\"calendar_date\", \"yyyy-MM-dd HH:mm:ss\"))\n\n#Convert to Pandas\ndf_cal_filter = df_cal_filter.toPandas()\ndf_cal_filter[\"calendar_date\"] = pd.to_datetime(df_cal_filter[\"calendar_date\"])\ndf_opb_sim_filter = df_opb_sim_filter.toPandas()\ndf_cal_filter = df_cal_filter.sort_values(by='calendar_date', ascending=True)\ndf_opb_sim_filter = df_opb_sim_filter.sort_values(by='priority_number', ascending=True)\n\n\n## CALCULATE\ndf_calculate = pd.DataFrame()\n\n# Mengurutkan dan mengambil nilai pertama\ndate = df_cal_filter.sort_values(\"calendar_date\").iloc[0][\"calendar_date\"]\nprioMax = df_opb_sim_filter.sort_values(\"priority_number\", ascending=False).iloc[0]['priority_number']\nprio = df_opb_sim_filter.sort_values(\"priority_number\").iloc[0][\"priority_number\"]\n\nhasilJam = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hasil_jam\"]\nplanHrs = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"plan/hrs\"]\noutHr= df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"output_hr\"]\nsimQty = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"simulation_qty\"]\nplanQty = (planHrs*outHr)\n\nwhile prio <= prioMax:\n    outHr= df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"output_hr\"]\n    liburFlag = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"libur_flag\"]\n    machine = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"machine\"]\n    if prio in df_opb_sim_filter[\"priority_number\"].values:\n        if liburFlag == \"N\":\n            if (hasilJam-planHrs>=0):\n                hasilJam = hasilJam - planHrs\n                sisa = 0 if hasilJam<=0 else hasilJam\n                outHr= df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"output_hr\"]\n                simQty = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"simulation_qty\"]\n                planQty = (planHrs*outHr)\n                asOfWeek = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"as_of_week\"]\n                opbSim = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"opb_simulation_id\"]\n                opbDtl = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"opb_dtl_id\"]\n                createDate = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"creation_date\"]\n                \n                hari = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hari\"]\n                new_rows = pd.DataFrame({\"machine\": [machine], \"as_of_week\":[asOfWeek], \"priority_number\": [prio],\"opb_simulation_id\": [opbSim],\"opb_dtl_id\":[opbDtl], \"simulation_qty\": [simQty], \"plan_qty\": [planQty], \"plan_hr\": [planHrs], \"calendar_date\": [date], \"hari\": [hari], \"sisa_jam\": [sisa], \"creation_date\": [createDate]})\n                df_calculate = pd.concat([df_calculate, new_rows])\n                prio=prio+1\n                if prio in df_opb_sim_filter[\"priority_number\"].values:\n                    if prio<=prioMax:\n                        planHrs = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"plan/hrs\"]\n                    else:\n                        planHrs=planHrs\n                else:\n                    prio+=1\n                    \n                    \n            elif (hasilJam-planHrs<0):\n                z= planHrs-hasilJam\n                planHrs= z%planHrs\n                sisa = 0\n                outHr = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"output_hr\"]\n                simQty = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"simulation_qty\"]\n                planQty = (hasilJam*outHr)\n                asOfWeek = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"as_of_week\"]\n                opbSim = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"opb_simulation_id\"]\n                opbDtl = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"opb_dtl_id\"]\n                createDate = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"creation_date\"]\n                \n                new_rows = pd.DataFrame({\"machine\": [machine],\"as_of_week\":[asOfWeek], \"priority_number\": [prio],\"opb_simulation_id\": [opbSim],\"opb_dtl_id\":[opbDtl], \"simulation_qty\": [simQty], \"plan_qty\": [planQty], \"plan_hr\": [hasilJam], \"calendar_date\": [date], \"hari\": [hari], \"sisa_jam\": [sisa],\"creation_date\": [createDate]})\n                df_calculate = pd.concat([df_calculate, new_rows])\n                date = date + timedelta(days = 1)\n                hari = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hari\"]\n                hasilJam = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hasil_jam\"]\n            else:\n                prio=prio+1\n        else: \n            hari = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hari\"]\n            new_rows = pd.DataFrame({\"machine\": [machine], \"as_of_week\":[asOfWeek], \"priority_number\": [prio],\"opb_simulation_id\": [opbSim],\"opb_dtl_id\":[opbDtl], \"simulation_qty\": 0, \"plan_qty\": 0, \"plan_hr\": 0, \"calendar_date\": [date], \"hari\": [hari], \"sisa_jam\": 0,\"creation_date\": [createDate]})\n            df_calculate = pd.concat([df_calculate, new_rows])\n        \n            date = date + timedelta(days = 1)\n            dateCheck = df_cal_filter.sort_values(\"calendar_date\",ascending=False).iloc[0][\"calendar_date\"]\n            if date > dateCheck:\n                break\n            else:\n                hari = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hari\"]\n                hasilJam = df_cal_filter[df_cal_filter[\"calendar_date\"] == date].iloc[0][\"hasil_jam\"]\n    else:\n        prio+=1\n        if prio in df_opb_sim_filter[\"priority_number\"].values:\n            if prio<=prioMax:\n                planHrs = df_opb_sim_filter[df_opb_sim_filter[\"priority_number\"] == prio].iloc[0][\"plan/hrs\"]\n            else:\n                planHrs=planHrs\n        else:\n            prio+=1\nspark.conf.set(\"spark.sql.execution.arrow.pyspark.enabled\", \"true\")\n\n# Mengubah DataFrame Pandas menjadi DataFrame PySpark\nresult_calculation = spark.createDataFrame(df_calculate)\nresult_calculation.printSchema()\nresult_calculation.write.format(\"parquet\") \\\n                .mode(\"overwrite\") \\\n                .saveAsTable(f\"dev_jip_simulasi.{machine_name}_simulation\")"

    # set name Machine
    text = text.replace("MACHINE_CODE_NAME_JIP", f"{machine_name}")

    # set as of week
    text = text.replace("AS_OF_WEEK_CODE_JIP", f"{as_of_week}")

    data['paragraphs'][0]['text'] = text
    data['name'] = f"{machine_name}-{as_of_week} {date}"

    return data

def combine_notebook():
    
    template = """
    {
        "name": "combine_result",
        "defaultInterpreterGroup": "spark_jip",
        "paragraphs": [
                {
                "text": ""
                }
            ]
        }
    """
    
    text = "%spark_jip.pyspark\ndatabase = \"dev_jip_simulasi\"\ndf = spark.sql(\"show tables in dev_jip_simulasi\")\n\ndf_table = df.select(\"tablename\")\n# df_table.show()\n\ntable_names = df_table.rdd.map(lambda row: row[0]).collect()\nresult_df = None \n\ndatabase_name = \"dev_jip\"\nif spark._jsparkSession.catalog().databaseExists(database_name):\n    print(f\"Database {database_name} Exists\")\nelse :\n    print(f\"create database {database_name} ...\")\n    spark.sql(f\"CREATE DATABASE {database_name}\")\n    \n# Iterate over the table names\nfor table_name in table_names:\n    # Add Database \n    table_name = database + \".\" + table_name\n    print(table_name)\n    \n    # Check if the table exists\n    if spark._jsparkSession.catalog().tableExists(table_name):\n        # Read data from the table\n        spark.sql(f\"REFRESH TABLE {table_name}\")\n        current_table_df = spark.read.table(table_name)\n        \n        # Union with the result DataFrame\n        if result_df is None: \n            result_df = current_table_df\n        else:\n            result_df = result_df.union(current_table_df)\n    else:\n        print(f\"Table {table_name} does not exist.\")\n        \nif result_df is not None:\n    result_df.write.format(\"parquet\") \\\n        .mode(\"overwrite\") \\\n        .saveAsTable(f\"{database_name}.result_simulation\")\n        \n    print(\"Create Table Success\")\nelse:\n    print(\"No tables available for union\")\n"

    logging.debug("[script combine] Load template json..")
    data = json.loads(template)
    logging.debug("[script combine] Load template json success..")
    
    data['paragraphs'][0]['text'] = text

    return data

def sample_notebook():
    script_test = {
        "name": "calculate",
        "defaultInterpreterGroup": "python",
        "paragraphs": [
            {
            "title": "Testing Wait",
            "text": "%python\nimport time\n\ntime.sleep(5)"
            }
        ]
    }
    
    return dict(script_test)