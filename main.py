from pyspark.sql import SparkSession,SQLContext
import pyspark.sql.functions as sf


class Operations:
    def __init__(self):
        self.url_conn = "jdbc:mysql://ec2-13-233-59-180.ap-south-1.compute.amazonaws.com:3306/db_crimes_dash"

    def __save_raw_to_hdfs(self, df):
        pass

    def save_raw_hdfs(self, df):
        self.__save_raw_to_hdfs(df)

    def __save_tables_to_mysql(self, df_unique, df_year, df_location, df_crimetype):
        # df_unique.write.format('jdbc').options(
        #       url=self.url_conn,
        #       driver='com.mysql.jdbc.Driver',
        #       dbtable='tbl_crimes',
        #       user='dashboard_user',
        #       password='root').mode('append').save()
        # df_year.write.format('jdbc').options(
        #     url=self.url_conn,
        #     driver='com.mysql.jdbc.Driver',
        #     dbtable='tbl_crime_arrests',
        #     user='dashboard_user',
        #     password='root').mode('append').save()
        # df_location.write.format('jdbc').options(
        #     url=self.url_conn,
        #     driver='com.mysql.jdbc.Driver',
        #     dbtable='tbl_cr_by_location',
        #     user='dashboard_user',
        #     password='root').mode('append').save()
        df_crimetype.repartition(200).write.format('jdbc').options(
            url=self.url_conn,
            driver='com.mysql.jdbc.Driver',
            dbtable='tbl_cr_by_type',
            user='dashboard_user',
            password='root').mode('append').save()

    def save_tables_to_mysql(self, df_unique, df_year, df_location, df_crimetype):
        self.__save_tables_to_mysql(df_unique, df_year, df_location, df_crimetype)

    def parseLines(self, line):
        fields = line.split("	")
        ID = fields[0]
        CASE = fields[1]
        DATE = fields[2]
        PRIMARYTYPE = fields[5]
        LOCATION = fields[7]
        ARREST = fields[9]
        YEAR = fields[17]
        return (str(ID)+str(CASE), DATE, PRIMARYTYPE, LOCATION, ARREST, YEAR)

    def getColumns(self):
        return ['ID', 'Date','PrimaryType', 'Location', 'Arrest', 'Year']

if __name__ == '__main__':
    try:
        op = Operations()
        spark = SparkSession.builder.appName("Dashboard").enableHiveSupport().getOrCreate()
        csv_file = spark.sparkContext.textFile("../project_resources/Crimes_-_2001_to_Present.tsv")
        rdd_parsed = csv_file.map(op.parseLines)
        header = rdd_parsed.first()
        rdd_filtered = rdd_parsed.filter(lambda line: line != header)
        df_data = spark.createDataFrame(rdd_filtered,schema=op.getColumns())

        op.save_raw_hdfs(df_data)

        uni_year = df_data.select('Year').distinct()

        df_agg_arr_yr = df_data.where(df_data["Arrest"]=='true').groupBy("Year").agg(sf.count("ID").alias("CountByYear"))

        df_location= df_data.groupBy(["Location", "Year"]).agg(sf.count("ID").alias("CountByLocation"))

        df_crime_type = df_data.groupBy(["PrimaryType", "Year"]).agg(sf.count("ID").alias("CountByCrimeType"))

        op.save_tables_to_mysql(uni_year, df_agg_arr_yr, df_location, df_crime_type)
        # print(df_crime_type.show())

    except Exception as err:
        print("Exception Occured:",err)