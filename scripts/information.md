Broze Bucket Name - yt-data-pipeline-bronze-ap-1-dev 
silver Bucket Name - yt-data-pipeline-silver-ap-1-dev 
gold Bucket Name - yt-data-pipeline-gold-ap-1-dev 

Script Bucket - yt-data-pipeline-script-ap-1-dev 

SNS ARN :arn:aws:sns:ap-south-1:199731094214:yt-data-pipeline-alerts-dev:71e16b77-72d9-467a-b9f6-2b96fd67ee04

Glue Bronze - yt_pipeline_bronze_dev
Glue Silver - yt_pipeline_silver_dev
Glue Gold - yt_pipeline_gold_dev

--bronze_database yt_pipeline_bronze_dev
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-silver-ap-1-dev
--silver_database yt_pipeline_silver_dev
--silver_table clean_statistics

--silver_database  yt_pipeline_silver_dev
--gold_bucket     yt-data-pipeline-gold-ap-1-dev
--gold_database   yt_pipeline_gold_dev
