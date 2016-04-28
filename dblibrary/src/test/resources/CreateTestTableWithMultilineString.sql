CREATE TABLE TestTableMultiline (ID Integer, Message Varchar(255));
INSERT INTO TestTableMultiline VALUES (1, 'from pyspark import SparkContext

# This is a simple program.
if __name__ == "__main__":
  sc = SparkContext(appName="PySpark-Test")
  sc.stop()
');
