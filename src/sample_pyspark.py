from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
from pyspark.sql.functions import trim, udf
from db_connection import postgresdb_connect

spark =SparkSession.builder.appName("ReadInputFiles").getOrCreate()

#path to the Unity_golf_club
ugc_file_path="./src/unity_golf_club.csv"

#path to the US Softball league file
us_softball_file_path="./src/us_softball_league.tsv"

#path to Company data file
company_data_file_path="./src/companies.csv"

ugc_schema =  StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name",StringType(), True),
    StructField("dob",DateType(), True),
    StructField("company_id", IntegerType(), True),
    StructField("last_active",DateType(), True),
    StructField("score", IntegerType(), True),
    StructField("member_since", IntegerType(), True),
    StructField("state", StringType(), True)
])
unity_golf_club_df=spark.read.csv(ugc_file_path, header=True, schema=ugc_schema, dateFormat='yyyy/MM/dd')
# unity_golf_club_df.show()

us_softball_schema=StructType([
    StructField("name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("company_id", IntegerType(), True),
    StructField("last_active", DateType(), True),
    StructField("score", IntegerType(), True),
    StructField("joined_league", IntegerType(), True),
    StructField("us_state", StringType(), True)
])
us_softball_league_df=spark.read.csv(us_softball_file_path, sep='\t', header=True, schema=us_softball_schema, dateFormat='MM/dd/yyyy')

# state_abbreviation_udf=udf(get_state_abbreviation, StringType())
# us_softball_league_df=us_softball_league_df.withColumn("State",state_abbreviation_udf(us_softball_league_df["us_state"] ))
us_softball_league_df.show()


company_schema=StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
company_df=spark.read.csv(company_data_file_path, header=True, schema=company_schema)

company_df=company_df.withColumn("name", trim(company_df.name))
#company_df.show()

unity_golf_club_df.createGlobalTempView("unity_golf_club")
us_softball_league_df.createGlobalTempView("us_softball_league")
company_df.createGlobalTempView("company")

ugc_error_records_df=spark.sql("SELECT a.* from global_temp.unity_golf_club a \
                               left join global_temp.company c on c.id=a.company_id \
          where dob>last_active or Year(dob)>member_since or c.id is null ")
ugc_error_records_path='./spark-warehouse/audit/ugc_error_records.csv'
ugc_error_records_df.write.csv(ugc_error_records_path, header=True, mode='overwrite')


us_softball_league_error_records_df=spark.sql("Select a.* from global_temp.us_softball_league a \
                                              left join global_temp.company c on c.id=a.company_id\
                                              where date_of_birth > last_active or Year(date_of_birth)>joined_league\
                                              or c.id is null")
us_softball_league_error_records_df_path="./spark-warehouse/audit/us_softball_league_error_records.csv"
us_softball_league_error_records_df.write.csv(us_softball_league_error_records_df_path, header=True, mode='overwrite')







