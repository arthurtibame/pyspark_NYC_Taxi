from pyspark.sql.types import *
import pyspark.sql.functions as F

class DatetimeProcess:
    """[summary]

    Args:
        DatetimeProcess(pyspark df, datetime_col): 
    Funcs(customized UDF):        
        isWeekendUDF(dayOfTheWeek)
        isHolidayUDF(date, years, state):
        isWeekendUDF(dayOfTheWeek)
        isWeekendUDF(dayOfTheWeek)
        isWeekendUDF(dayOfTheWeek)   
    """
    def __init__(self, df, datetime_column):       
        self.df = df        
        self.datetime_column = datetime_column       
          
    def preprocess(self):
        # drop location is 0
        self.df = self.df.filter(self.df.Start_Lat  != 0.0 )\
            .filter(self.df.Start_Lon  != 0.0)\
            .filter(self.df.End_Lon  != 0.0)\
            .filter(self.df.End_Lat  != 0.0)

        self.df = self.df\
            .withColumn(self.datetime_columnname, F.unix_timestamp(F.col(self.datetime_columnname),"yyyy-MM-dd HH:mm:ss").cast(TimestampType()))\
            .withColumn("year", F.year(F.col(self.datetime_columnname)))\
            .withColumn("month", F.month(F.col(self.datetime_columnname)))\
            .withColumn("day", F.dayofmonth(F.col(self.datetime_columnname)))\
            .withColumn("hour", F.hour(F.col(self.datetime_columnname)))\
            .withColumn("hour", F.hour(F.col(self.datetime_columnname)))\
            .withColumn("minute", F.minute(F.col(self.datetime_columnname)))\
            .withColumn("Date", F.to_date(F.col(self.datetime_columnname)))\
            .withColumn("pickup_time", F.round(F.col("hour") + F.col("minute")/60))\
            .withColumn("dayOfTheWeek", F.dayofweek(F.col("Date")))\
            .withColumn("isWeekend", self.isWeekendUDF(F.col("dayOfTheWeek")))\
            .withColumn("isHoliday", self.isHolidayUDF(F.col("Date")))\
            .withColumn("isCashPaid", self.isHolidayUDF(F.col("Payment_Type")))       


    @staticmethod        
    @F.udf(returnType=BooleanType())
    def isWeekendUDF(dayOfTheWeek): 
        if dayOfTheWeek == 1 or dayOfTheWeek == 7:
            return True
        else:
            return False

    @staticmethod        
    @F.udf(returnType=BooleanType())
    def isHolidayUDF(date, years=2009, state="NY"):
        """[Based on holiday package to check whether the date is holiday or not]
        More Details (available country please check):
            https://pypi.org/project/holidays/

        Args:
            date ([type]): [description]
            years (int, optional): [description]. Defaults to 2009.
            state (str, optional): [description]. Defaults to "NY".

        Returns:
            Boolean: [True: the date is holiday, False: the date is not holiday]
        """
        import holidays
        us_holidays = list(holidays.US(years = years, state=state).keys())
        if date in us_holidays:
            return True
        else:
            return False

    @staticmethod                   
    @F.udf(returnType=BooleanType()) 
    def isCashPaidUDF(payment):
        """[summary]

        Args:
            payment ([string | int]): [payment method of NYC Taxi dataset]

        Returns:
            [Boolean]: [True: paid by cash, False: paid by credit card]
        """
        cash_list = ['CSH', 'CASH', 1]
        if payment in cash_list:
            return True
        else:
            return False
